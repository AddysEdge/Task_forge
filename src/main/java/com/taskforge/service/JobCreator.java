package com.taskforge.service;

import com.taskforge.config.TaskForgeProperties;
import com.taskforge.dto.CreateJobRequest;
import com.taskforge.entity.JobEntity;
import com.taskforge.kafka.JobEvent;
import com.taskforge.model.JobStatus;
import com.taskforge.outbox.OutboxWriter;
import com.taskforge.repository.JobRepository;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

import java.time.Instant;

/**
 * The transactional half of job submission, kept in its own bean on purpose.
 *
 * <p>Two things happen here and they must happen together: the job row is written, and the
 * outbox event announcing it is written. If the process dies immediately after commit, the
 * job is still guaranteed to reach a worker, because the announcement is already durable in
 * the same database.
 *
 * <p>It lives apart from {@link JobService} so the unique-constraint violation that resolves
 * an idempotency race rolls back <em>this</em> transaction only. Catching it inside the same
 * transaction would leave that transaction marked rollback-only and make the recovery read
 * fail too.
 */
@Service
public class JobCreator {

    private final JobRepository jobRepository;
    private final OutboxWriter outboxWriter;
    private final TaskForgeProperties properties;

    public JobCreator(JobRepository jobRepository, OutboxWriter outboxWriter, TaskForgeProperties properties) {
        this.jobRepository = jobRepository;
        this.outboxWriter = outboxWriter;
        this.properties = properties;
    }

    @Transactional(propagation = Propagation.REQUIRES_NEW)
    public JobEntity createWithOutboxEvent(CreateJobRequest request, String payloadJson) {
        Instant now = Instant.now();

        JobEntity job = JobEntity.builder()
                .idempotencyKey(request.idempotencyKey())
                .jobType(request.jobType())
                .payload(payloadJson)
                .status(JobStatus.QUEUED)
                .createdAt(now)
                .updatedAt(now)
                .retryCount(0)
                .maxRetries(request.maxRetries() != null
                        ? request.maxRetries()
                        : properties.getRetry().getDefaultMaxRetries())
                .timeoutSeconds(request.timeoutSeconds() != null
                        ? request.timeoutSeconds()
                        : properties.getLimits().getDefaultTimeoutSeconds())
                .build();

        // saveAndFlush so a duplicate idempotency key surfaces here, inside this
        // transaction, rather than at some later flush point.
        JobEntity saved = jobRepository.saveAndFlush(job);

        outboxWriter.append(
                properties.getTopics().getJobs(),
                JobEvent.submitted(saved.getId(), saved.getJobType(), saved.currentAttempt()));

        return saved;
    }
}
