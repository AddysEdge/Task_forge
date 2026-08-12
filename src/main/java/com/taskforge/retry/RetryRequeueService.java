package com.taskforge.retry;

import com.taskforge.cache.JobCache;
import com.taskforge.config.TaskForgeProperties;
import com.taskforge.entity.JobEntity;
import com.taskforge.kafka.JobEvent;
import com.taskforge.outbox.OutboxWriter;
import com.taskforge.repository.JobRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.time.Instant;
import java.util.List;
import java.util.UUID;

/**
 * Turns elapsed backoff into a fresh dispatch.
 *
 * <p>This is how TaskForge waits between attempts without anything actually waiting. A failed
 * job is stored as RETRYING with a {@code next_attempt_at} timestamp; this scan picks up rows
 * whose time has come, flips them back to QUEUED and announces them on the retry topic. No
 * consumer thread sleeps, no in-memory timer has to survive, and the schedule is unaffected
 * by any process restarting — the retry count and the due time are both already in
 * PostgreSQL.
 *
 * <p>The re-announcement goes through the outbox in the same transaction as the status
 * change, so a job can never be marked QUEUED without an event that will eventually reach a
 * worker.
 */
@Service
public class RetryRequeueService {

    private static final Logger log = LoggerFactory.getLogger(RetryRequeueService.class);

    private final JobRepository jobRepository;
    private final OutboxWriter outboxWriter;
    private final JobCache jobCache;
    private final TaskForgeProperties properties;

    public RetryRequeueService(JobRepository jobRepository,
                               OutboxWriter outboxWriter,
                               JobCache jobCache,
                               TaskForgeProperties properties) {
        this.jobRepository = jobRepository;
        this.outboxWriter = outboxWriter;
        this.jobCache = jobCache;
        this.properties = properties;
    }

    /**
     * Re-queues every job whose backoff has elapsed.
     *
     * <p>Rows are taken with {@code FOR UPDATE SKIP LOCKED}, so this can run on every
     * instance at once without two of them re-queueing the same job.
     *
     * @return how many jobs were re-queued
     */
    @Transactional
    public int requeueDueJobs() {
        Instant now = Instant.now();
        List<UUID> dueJobIds = jobRepository.lockRetryableJobs(now, properties.getRetry().getBatchSize());
        if (dueJobIds.isEmpty()) {
            return 0;
        }

        int requeued = 0;
        for (UUID jobId : dueJobIds) {
            JobEntity job = jobRepository.findById(jobId).orElse(null);
            if (job == null) {
                continue;
            }
            if (jobRepository.requeueFromRetrying(jobId, now) == 0) {
                // Cancelled while it sat in backoff, most likely.
                continue;
            }

            jobCache.invalidate(jobId);
            outboxWriter.append(
                    properties.getTopics().getRetry(),
                    JobEvent.retryScheduled(jobId, job.getJobType(), job.currentAttempt()));

            log.info("Job re-queued for retry jobId={} attempt={} retryCount={} of {}",
                    jobId, job.currentAttempt(), job.getRetryCount(), job.getMaxRetries());
            requeued++;
        }
        return requeued;
    }
}
