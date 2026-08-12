package com.taskforge.validation;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.taskforge.config.TaskForgeProperties;
import com.taskforge.exception.PayloadValidationException;
import com.taskforge.execution.JobHandlerRegistry;
import com.taskforge.model.JobType;
import org.springframework.stereotype.Component;

import java.nio.charset.StandardCharsets;

/**
 * Validates a submitted payload before anything is written, so a job that could never
 * execute is rejected with HTTP 400 instead of being queued, dispatched, failed and retried.
 *
 * <p>The shape check is delegated to the handler that will actually run the job, which keeps
 * knowledge of each job type in exactly one place.
 */
@Component
public class JobPayloadValidator {

    private final JobHandlerRegistry handlerRegistry;
    private final ObjectMapper objectMapper;
    private final TaskForgeProperties properties;

    public JobPayloadValidator(JobHandlerRegistry handlerRegistry,
                               ObjectMapper objectMapper,
                               TaskForgeProperties properties) {
        this.handlerRegistry = handlerRegistry;
        this.objectMapper = objectMapper;
        this.properties = properties;
    }

    /**
     * @return the canonical JSON text to persist
     * @throws PayloadValidationException if the payload is the wrong shape or too large
     */
    public String validateAndSerialize(JobType jobType, JsonNode payload) {
        PayloadFields.requireObject(payload);

        String serialized;
        try {
            serialized = objectMapper.writeValueAsString(payload);
        } catch (JsonProcessingException e) {
            throw new PayloadValidationException("payload could not be serialised: " + e.getOriginalMessage());
        }

        int maxBytes = properties.getLimits().getMaxPayloadBytes();
        int actualBytes = serialized.getBytes(StandardCharsets.UTF_8).length;
        if (actualBytes > maxBytes) {
            throw new PayloadValidationException(
                    "payload is %d bytes, which exceeds the %d byte limit".formatted(actualBytes, maxBytes));
        }

        handlerRegistry.handlerFor(jobType).validatePayload(payload);
        return serialized;
    }
}
