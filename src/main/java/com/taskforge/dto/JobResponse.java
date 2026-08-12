package com.taskforge.dto;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.databind.JsonNode;
import com.taskforge.model.JobStatus;
import com.taskforge.model.JobType;

import java.time.Instant;
import java.util.UUID;

/**
 * Client-facing view of a job. Kept separate from {@link com.taskforge.entity.JobEntity} so
 * the persistence model can evolve without changing the API contract, and so no JPA proxy
 * or lazy association ever reaches the HTTP layer.
 *
 * <p>This is also the exact shape cached in Redis.
 */
@JsonInclude(JsonInclude.Include.NON_NULL)
public record JobResponse(
        UUID id,
        String idempotencyKey,
        JobType jobType,
        JobStatus status,
        JsonNode payload,
        Instant createdAt,
        Instant updatedAt,
        Instant startedAt,
        Instant completedAt,
        int attempt,
        int retryCount,
        int maxRetries,
        int timeoutSeconds,
        Instant nextAttemptAt,
        String workerId,
        Instant leaseExpiresAt,
        JsonNode result,
        String errorMessage,
        long version) {
}
