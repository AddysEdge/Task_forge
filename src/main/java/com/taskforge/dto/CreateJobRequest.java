package com.taskforge.dto;

import com.fasterxml.jackson.databind.JsonNode;
import com.taskforge.model.JobType;
import jakarta.validation.constraints.Max;
import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.Size;

/**
 * Job submission body.
 *
 * <p>The payload is bound as a {@link JsonNode} rather than a polymorphic Java type on
 * purpose: it keeps arbitrary class instantiation off the deserialization path. Its size
 * and per-job-type shape are checked by
 * {@link com.taskforge.validation.JobPayloadValidator} before anything is persisted.
 */
public record CreateJobRequest(

        @NotBlank(message = "idempotencyKey is required")
        @Size(min = 1, max = 200, message = "idempotencyKey must be between 1 and 200 characters")
        String idempotencyKey,

        @NotNull(message = "jobType is required and must be one of DELAY, COMPUTE, FAIL_TEST")
        JobType jobType,

        @NotNull(message = "payload is required")
        JsonNode payload,

        @Min(value = 0, message = "maxRetries must be between 0 and 10")
        @Max(value = 10, message = "maxRetries must be between 0 and 10")
        Integer maxRetries,

        @Min(value = 1, message = "timeoutSeconds must be between 1 and 3600")
        @Max(value = 3600, message = "timeoutSeconds must be between 1 and 3600")
        Integer timeoutSeconds) {
}
