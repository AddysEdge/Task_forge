package com.taskforge.support;

import com.taskforge.entity.JobEntity;
import com.taskforge.model.JobStatus;
import com.taskforge.model.JobType;

import java.time.Instant;
import java.util.UUID;

/** Builders for jobs in states the application would normally only reach through SQL. */
public final class JobFixtures {

    private JobFixtures() {
    }

    public static JobEntity queued(JobType type, String payload) {
        return JobEntity.builder()
                .idempotencyKey("key-" + UUID.randomUUID())
                .jobType(type)
                .payload(payload)
                .status(JobStatus.QUEUED)
                .maxRetries(3)
                .timeoutSeconds(60)
                .build();
    }

    public static JobEntity queuedDelay() {
        return queued(JobType.DELAY, "{\"durationMs\":50}");
    }

    /** A job that a worker has claimed, with an explicit lease deadline. */
    public static JobEntity running(String workerId, Instant startedAt, Instant leaseExpiresAt, int timeoutSeconds) {
        return JobEntity.builder()
                .idempotencyKey("key-" + UUID.randomUUID())
                .jobType(JobType.DELAY)
                .payload("{\"durationMs\":50}")
                .status(JobStatus.RUNNING)
                .maxRetries(3)
                .timeoutSeconds(timeoutSeconds)
                .workerId(workerId)
                .startedAt(startedAt)
                .leaseExpiresAt(leaseExpiresAt)
                .build();
    }
}
