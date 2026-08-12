package com.taskforge.kafka;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.taskforge.model.JobType;

import java.time.Instant;
import java.util.UUID;

/**
 * The message TaskForge puts on Kafka. Note what it is <em>not</em>: it carries no job
 * payload, no result and no status the worker is expected to trust.
 *
 * <p>It is a notification that a job with this id is worth looking at. The worker always
 * re-reads authoritative state from PostgreSQL before doing anything, which is what makes
 * duplicate and out-of-order delivery harmless.
 *
 * <p>Serialised as plain JSON with unknown fields ignored, so an older worker can consume
 * events produced by a newer API without failing, and no class name from the wire is ever
 * used to choose a Java type to instantiate.
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public record JobEvent(
        UUID eventId,
        UUID jobId,
        JobType jobType,
        JobEventType eventType,
        int attempt,
        Instant occurredAt,
        String errorMessage) {

    public static JobEvent submitted(UUID jobId, JobType jobType, int attempt) {
        return new JobEvent(UUID.randomUUID(), jobId, jobType, JobEventType.JOB_SUBMITTED,
                attempt, Instant.now(), null);
    }

    public static JobEvent retryScheduled(UUID jobId, JobType jobType, int attempt) {
        return new JobEvent(UUID.randomUUID(), jobId, jobType, JobEventType.JOB_RETRY_SCHEDULED,
                attempt, Instant.now(), null);
    }

    public static JobEvent deadLettered(UUID jobId, JobType jobType, int attempt, String errorMessage) {
        return new JobEvent(UUID.randomUUID(), jobId, jobType, JobEventType.JOB_DEAD_LETTERED,
                attempt, Instant.now(), errorMessage);
    }
}
