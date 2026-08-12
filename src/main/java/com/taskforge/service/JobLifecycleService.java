package com.taskforge.service;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.taskforge.cache.JobCache;
import com.taskforge.config.TaskForgeProperties;
import com.taskforge.entity.JobEntity;
import com.taskforge.kafka.JobEvent;
import com.taskforge.model.JobStatus;
import com.taskforge.outbox.OutboxWriter;
import com.taskforge.repository.JobRepository;
import com.taskforge.retry.BackoffCalculator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.time.Duration;
import java.time.Instant;
import java.util.Optional;
import java.util.UUID;

/**
 * Every durable job state change a worker or the recovery scan can make.
 *
 * <p>Each method is one transaction wrapping one conditional UPDATE. They all return a
 * boolean rather than throwing when the update matches no rows, because "no rows" is a
 * normal outcome: it means the job was cancelled, recovered, or finished by someone else
 * while this attempt was in flight. Acting on that honestly — by doing nothing — is what
 * makes duplicate Kafka delivery and overlapping recovery safe.
 *
 * <p>Cache invalidation is registered inside these transactions and executes after commit,
 * so Redis can never be left advertising a state PostgreSQL did not keep.
 */
@Service
public class JobLifecycleService {

    private static final Logger log = LoggerFactory.getLogger(JobLifecycleService.class);
    private static final int MAX_ERROR_LENGTH = 2000;

    private final JobRepository jobRepository;
    private final JobCache jobCache;
    private final OutboxWriter outboxWriter;
    private final BackoffCalculator backoffCalculator;
    private final TaskForgeProperties properties;
    private final ObjectMapper objectMapper;

    public JobLifecycleService(JobRepository jobRepository,
                               JobCache jobCache,
                               OutboxWriter outboxWriter,
                               BackoffCalculator backoffCalculator,
                               TaskForgeProperties properties,
                               ObjectMapper objectMapper) {
        this.jobRepository = jobRepository;
        this.jobCache = jobCache;
        this.outboxWriter = outboxWriter;
        this.backoffCalculator = backoffCalculator;
        this.properties = properties;
        this.objectMapper = objectMapper;
    }

    /**
     * Takes exclusive ownership of a QUEUED job.
     *
     * <p>This is the single point at which duplicate Kafka delivery is neutralised. Two
     * workers handed the same message both run this statement; PostgreSQL serialises them on
     * the row, the first flips QUEUED to RUNNING, and the second matches nothing. A job that
     * already completed matches nothing either. Only a caller that receives a non-empty
     * result may execute.
     *
     * @return the freshly read job if this caller won the claim, otherwise empty
     */
    @Transactional
    public Optional<JobEntity> claim(UUID jobId, String workerId) {
        Instant now = Instant.now();
        Instant leaseExpiresAt = now.plus(properties.getWorker().getLeaseDuration());

        int claimed = jobRepository.claim(jobId, workerId, now, leaseExpiresAt);
        if (claimed == 0) {
            return Optional.empty();
        }

        jobCache.invalidate(jobId);
        Optional<JobEntity> job = jobRepository.findById(jobId);
        job.ifPresent(j -> log.info("Worker claimed job jobId={} workerId={} attempt={} leaseExpiresAt={}",
                jobId, workerId, j.currentAttempt(), leaseExpiresAt));
        return job;
    }

    @Transactional
    public boolean renewLease(UUID jobId, String workerId) {
        Instant now = Instant.now();
        Instant leaseExpiresAt = now.plus(properties.getWorker().getLeaseDuration());
        return jobRepository.renewLease(jobId, workerId, now, leaseExpiresAt) > 0;
    }

    @Transactional
    public boolean markCompleted(UUID jobId, String workerId, JsonNode result, long executionMillis) {
        String resultJson = serialiseResult(jobId, result);
        boolean applied = jobRepository.markCompleted(jobId, workerId, resultJson, Instant.now()) > 0;
        if (applied) {
            jobCache.invalidate(jobId);
            log.info("Job completed jobId={} workerId={} executionTimeMs={}", jobId, workerId, executionMillis);
        } else {
            log.warn("Completion ignored for jobId={} workerId={}: ownership or state changed underneath",
                    jobId, workerId);
        }
        return applied;
    }

    /**
     * Records a transient failure: schedules the next attempt if the budget allows, otherwise
     * dead-letters the job.
     */
    @Transactional
    public boolean markRetryableFailure(JobEntity job, String workerId, String errorMessage) {
        Instant now = Instant.now();
        String error = truncate(errorMessage);

        if (job.getRetryCount() >= job.getMaxRetries()) {
            return deadLetter(job, workerId, error, now);
        }

        int retryNumber = job.getRetryCount() + 1;
        Duration backoff = backoffCalculator.delayForRetry(retryNumber);
        Instant nextAttemptAt = now.plus(backoff);

        boolean applied = jobRepository.markRetrying(job.getId(), workerId, error, nextAttemptAt, now) > 0;
        if (applied) {
            jobCache.invalidate(job.getId());
            log.info("Retry scheduled jobId={} workerId={} attempt={} retry={} of {} backoffMs={} nextAttemptAt={}",
                    job.getId(), workerId, job.currentAttempt(), retryNumber, job.getMaxRetries(),
                    backoff.toMillis(), nextAttemptAt);
        } else {
            log.warn("Retry scheduling ignored for jobId={} workerId={}: state changed underneath",
                    job.getId(), workerId);
        }
        return applied;
    }

    @Transactional
    public boolean markPermanentFailure(JobEntity job, String workerId, String errorMessage) {
        boolean applied = jobRepository.markFailed(
                job.getId(), workerId, truncate(errorMessage), Instant.now()) > 0;
        if (applied) {
            jobCache.invalidate(job.getId());
            log.info("Job failed permanently jobId={} workerId={} attempt={} (no retry attempted)",
                    job.getId(), workerId, job.currentAttempt());
        }
        return applied;
    }

    @Transactional
    public boolean markCancelled(UUID jobId, String workerId) {
        boolean applied = jobRepository.markCancelledByWorker(
                jobId, workerId, "Cancelled on request during execution", Instant.now()) > 0;
        if (applied) {
            jobCache.invalidate(jobId);
            log.info("Job cancelled jobId={} workerId={}", jobId, workerId);
        }
        return applied;
    }

    /**
     * Handles a job whose own worker noticed it blew its deadline: TIMED_OUT first, then the
     * same retry-or-dead-letter decision recovery would make.
     */
    @Transactional
    public boolean markTimedOut(JobEntity job, String workerId) {
        Instant now = Instant.now();
        String error = "Execution exceeded its timeout of %d seconds".formatted(job.getTimeoutSeconds());

        if (jobRepository.markTimedOutByWorker(job.getId(), workerId, error, now) == 0) {
            log.warn("Timeout transition ignored for jobId={} workerId={}: state changed underneath",
                    job.getId(), workerId);
            return false;
        }
        jobCache.invalidate(job.getId());
        log.warn("Timeout detected jobId={} workerId={} attempt={} timeoutSeconds={}",
                job.getId(), workerId, job.currentAttempt(), job.getTimeoutSeconds());

        resolveTimedOut(job, error, now);
        return true;
    }

    /**
     * Moves a TIMED_OUT job onward. Shared by the in-process watchdog and the recovery scan
     * so an abandoned job and a slow one follow exactly the same path.
     */
    @Transactional
    public void resolveTimedOut(JobEntity job, String errorMessage, Instant now) {
        if (job.getRetryCount() >= job.getMaxRetries()) {
            String error = truncate(errorMessage + " (retry budget exhausted)");
            if (jobRepository.deadLetterFromTimedOut(job.getId(), error, now) > 0) {
                jobCache.invalidate(job.getId());
                publishDeadLetterEvent(job, error);
                log.error("Job dead-lettered after timeout jobId={} attempts={} maxRetries={}",
                        job.getId(), job.currentAttempt(), job.getMaxRetries());
            }
            return;
        }

        int retryNumber = job.getRetryCount() + 1;
        Duration backoff = backoffCalculator.delayForRetry(retryNumber);
        Instant nextAttemptAt = now.plus(backoff);

        if (jobRepository.retryFromTimedOut(job.getId(), nextAttemptAt, now) > 0) {
            jobCache.invalidate(job.getId());
            log.info("Retry scheduled after timeout jobId={} retry={} of {} backoffMs={} nextAttemptAt={}",
                    job.getId(), retryNumber, job.getMaxRetries(), backoff.toMillis(), nextAttemptAt);
        }
    }

    private boolean deadLetter(JobEntity job, String workerId, String error, Instant now) {
        String message = truncate(error + " (retry budget exhausted)");
        boolean applied = jobRepository.markDeadLettered(job.getId(), workerId, message, now) > 0;
        if (applied) {
            jobCache.invalidate(job.getId());
            publishDeadLetterEvent(job, message);
            log.error("Job dead-lettered jobId={} workerId={} attempts={} maxRetries={} error={}",
                    job.getId(), workerId, job.currentAttempt(), job.getMaxRetries(), message);
        }
        return applied;
    }

    /**
     * The DLQ notification goes through the outbox in the same transaction as the
     * DEAD_LETTERED update, so the two cannot disagree.
     */
    private void publishDeadLetterEvent(JobEntity job, String errorMessage) {
        outboxWriter.append(
                properties.getTopics().getDlq(),
                JobEvent.deadLettered(job.getId(), job.getJobType(), job.currentAttempt(), errorMessage));
    }

    private String serialiseResult(UUID jobId, JsonNode result) {
        if (result == null) {
            return null;
        }
        try {
            return objectMapper.writeValueAsString(result);
        } catch (JsonProcessingException e) {
            log.error("jobId={} produced an unserialisable result; storing null: {}", jobId, e.getMessage());
            return null;
        }
    }

    private String truncate(String message) {
        if (message == null) {
            return "Unknown error";
        }
        return message.length() <= MAX_ERROR_LENGTH ? message : message.substring(0, MAX_ERROR_LENGTH);
    }

    /** Exposed for recovery, which needs the row it just locked. */
    @Transactional(readOnly = true)
    public Optional<JobEntity> find(UUID jobId) {
        return jobRepository.findById(jobId);
    }

    public boolean isTerminal(JobStatus status) {
        return status != null && status.isTerminal();
    }
}
