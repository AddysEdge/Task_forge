package com.taskforge.service;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.taskforge.dto.JobResponse;
import com.taskforge.entity.JobEntity;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

/** Translates the persistence model into the API/cache model. */
@Component
public class JobMapper {

    private static final Logger log = LoggerFactory.getLogger(JobMapper.class);

    private final ObjectMapper objectMapper;

    public JobMapper(ObjectMapper objectMapper) {
        this.objectMapper = objectMapper;
    }

    public JobResponse toResponse(JobEntity job) {
        return new JobResponse(
                job.getId(),
                job.getIdempotencyKey(),
                job.getJobType(),
                job.getStatus(),
                readJson(job.getPayload(), job.getId().toString(), "payload"),
                job.getCreatedAt(),
                job.getUpdatedAt(),
                job.getStartedAt(),
                job.getCompletedAt(),
                job.currentAttempt(),
                job.getRetryCount(),
                job.getMaxRetries(),
                job.getTimeoutSeconds(),
                job.getNextAttemptAt(),
                job.getWorkerId(),
                job.getLeaseExpiresAt(),
                readJson(job.getResult(), job.getId().toString(), "result"),
                job.getErrorMessage(),
                job.getVersion());
    }

    private JsonNode readJson(String raw, String jobId, String field) {
        if (raw == null) {
            return null;
        }
        try {
            return objectMapper.readTree(raw);
        } catch (JsonProcessingException e) {
            // The column is jsonb, so this should be unreachable; surface it loudly rather
            // than failing the whole request over one unreadable field.
            log.error("jobId={} has unparseable {} JSON in the database: {}", jobId, field, e.getMessage());
            return null;
        }
    }
}
