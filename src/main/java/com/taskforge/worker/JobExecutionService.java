package com.taskforge.worker;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.taskforge.config.WorkerIdentity;
import com.taskforge.entity.JobEntity;
import com.taskforge.exception.PayloadValidationException;
import com.taskforge.execution.JobExecutionContext;
import com.taskforge.execution.JobExecutionResult;
import com.taskforge.execution.JobHandler;
import com.taskforge.execution.JobHandlerRegistry;
import com.taskforge.execution.JobStoppedException;
import com.taskforge.execution.PermanentJobFailureException;
import com.taskforge.execution.StopReason;
import com.taskforge.service.JobLifecycleService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import java.time.Instant;
import java.util.Optional;
import java.util.UUID;

/**
 * Runs one job attempt, start to finish.
 *
 * <p>The ordering here is the whole design. Receiving a Kafka message grants nothing: the
 * first thing this does is ask PostgreSQL to atomically flip the job from QUEUED to RUNNING.
 * Only a worker that wins that statement executes anything. A duplicate message, a message
 * for a job someone else already took, and a message for a job that finished long ago all
 * take the same early exit.
 *
 * <p>Deliberately not transactional: a database transaction must never be held open across
 * arbitrary user code that may run for minutes. Each state change is its own short
 * transaction inside {@link JobLifecycleService}.
 */
@Service
public class JobExecutionService {

    private static final Logger log = LoggerFactory.getLogger(JobExecutionService.class);

    private final JobLifecycleService jobLifecycleService;
    private final JobHandlerRegistry handlerRegistry;
    private final RunningJobRegistry runningJobRegistry;
    private final WorkerIdentity workerIdentity;
    private final ObjectMapper objectMapper;

    public JobExecutionService(JobLifecycleService jobLifecycleService,
                               JobHandlerRegistry handlerRegistry,
                               RunningJobRegistry runningJobRegistry,
                               WorkerIdentity workerIdentity,
                               ObjectMapper objectMapper) {
        this.jobLifecycleService = jobLifecycleService;
        this.handlerRegistry = handlerRegistry;
        this.runningJobRegistry = runningJobRegistry;
        this.workerIdentity = workerIdentity;
        this.objectMapper = objectMapper;
    }

    public void executeJob(UUID jobId) {
        String workerId = workerIdentity.workerId();

        Optional<JobEntity> claimed = jobLifecycleService.claim(jobId, workerId);
        if (claimed.isEmpty()) {
            logSkip(jobId, workerId);
            return;
        }

        JobEntity job = claimed.get();
        JobExecutionContext context = newContext(job, workerId);
        if (context == null) {
            return;
        }

        context.bindExecutingThread(Thread.currentThread());
        runningJobRegistry.register(context);

        long startedNanos = System.nanoTime();
        log.info("Job started jobId={} jobType={} workerId={} attempt={} timeoutSeconds={}",
                job.getId(), job.getJobType(), workerId, job.currentAttempt(), job.getTimeoutSeconds());

        try {
            JobHandler handler = handlerRegistry.handlerFor(job.getJobType());
            JobExecutionResult result = handler.execute(context);
            applyResult(job, workerId, result, elapsedMillis(startedNanos));

        } catch (JobStoppedException e) {
            handleStop(job, workerId, e.getReason());

        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            handleStop(job, workerId, Optional.ofNullable(context.stopReason()).orElse(StopReason.CANCELLED));

        } catch (PermanentJobFailureException e) {
            log.warn("Job failed permanently jobId={} workerId={}: {}", job.getId(), workerId, e.getMessage());
            jobLifecycleService.markPermanentFailure(job, workerId, e.getMessage());

        } catch (PayloadValidationException e) {
            // A payload this handler cannot read will not become readable on a retry.
            log.warn("Job has an unusable payload jobId={} workerId={}: {}", job.getId(), workerId, e.getMessage());
            jobLifecycleService.markPermanentFailure(job, workerId, "Invalid payload: " + e.getMessage());

        } catch (Exception e) {
            // A stop signal often surfaces as some other exception from inside library code.
            if (context.isStopRequested()) {
                handleStop(job, workerId, context.stopReason());
            } else {
                log.error("Job attempt failed jobId={} workerId={} attempt={}",
                        job.getId(), workerId, job.currentAttempt(), e);
                jobLifecycleService.markRetryableFailure(job, workerId, describe(e));
            }

        } finally {
            runningJobRegistry.unregister(job.getId());
            // Clear any lingering interrupt so this pooled thread is clean for the next job.
            Thread.interrupted();
        }
    }

    private void applyResult(JobEntity job, String workerId, JobExecutionResult result, long executionMillis) {
        if (result == null) {
            jobLifecycleService.markRetryableFailure(job, workerId, "Handler returned no result");
            return;
        }

        switch (result.outcome()) {
            case SUCCESS -> jobLifecycleService.markCompleted(job.getId(), workerId, result.result(), executionMillis);
            case RETRYABLE_FAILURE -> {
                log.warn("Job attempt failed (retryable) jobId={} workerId={} attempt={}: {}",
                        job.getId(), workerId, job.currentAttempt(), result.message());
                jobLifecycleService.markRetryableFailure(job, workerId, result.message());
            }
            case PERMANENT_FAILURE -> {
                log.warn("Job attempt failed (permanent) jobId={} workerId={} attempt={}: {}",
                        job.getId(), workerId, job.currentAttempt(), result.message());
                jobLifecycleService.markPermanentFailure(job, workerId, result.message());
            }
        }
    }

    private void handleStop(JobEntity job, String workerId, StopReason reason) {
        StopReason effective = reason == null ? StopReason.CANCELLED : reason;
        switch (effective) {
            case CANCELLED -> jobLifecycleService.markCancelled(job.getId(), workerId);
            case TIMED_OUT -> jobLifecycleService.markTimedOut(job, workerId);
            case LEASE_LOST ->
                // Ownership is gone; writing any terminal state here would be a lie, and the
                // worker_id guard on every update would reject it anyway.
                    log.warn("Abandoning jobId={} on workerId={}: lease was lost to recovery",
                            job.getId(), workerId);
        }
    }

    private JobExecutionContext newContext(JobEntity job, String workerId) {
        Instant startedAt = job.getStartedAt() != null ? job.getStartedAt() : Instant.now();
        Instant deadline = startedAt.plusSeconds(job.getTimeoutSeconds());

        JsonNode payload;
        try {
            payload = objectMapper.readTree(job.getPayload());
        } catch (JsonProcessingException e) {
            log.error("jobId={} has an unreadable payload; failing it permanently", job.getId());
            jobLifecycleService.markPermanentFailure(job, workerId, "Stored payload is not valid JSON");
            return null;
        }

        return new JobExecutionContext(
                job.getId(), job.getJobType(), payload, job.currentAttempt(), workerId, deadline);
    }

    /**
     * A failed claim is the normal case for duplicate delivery, so it is logged as
     * information rather than as a problem — with the reason, which is what makes the
     * behaviour visible in the demo.
     */
    private void logSkip(UUID jobId, String workerId) {
        String reason = jobLifecycleService.find(jobId)
                .map(job -> "status=" + job.getStatus()
                        + (job.getWorkerId() != null ? " owner=" + job.getWorkerId() : ""))
                .orElse("job no longer exists");
        log.info("Skipping jobId={} on workerId={}: not claimable ({})", jobId, workerId, reason);
    }

    private long elapsedMillis(long startedNanos) {
        return (System.nanoTime() - startedNanos) / 1_000_000;
    }

    /** Class name plus message only: enough to diagnose, without a stack trace in the database. */
    private String describe(Exception e) {
        String message = e.getMessage();
        return message == null || message.isBlank()
                ? e.getClass().getSimpleName()
                : e.getClass().getSimpleName() + ": " + message;
    }
}
