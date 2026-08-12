package com.taskforge.outbox;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.taskforge.entity.OutboxEventEntity;
import com.taskforge.kafka.JobEvent;
import com.taskforge.repository.OutboxEventRepository;
import org.springframework.stereotype.Component;

import java.time.Instant;

/**
 * Appends an event to the outbox <em>inside the caller's transaction</em>.
 *
 * <p>That is the whole point of the pattern: the job row and the intent to announce it are
 * one atomic write. There is no window in which a job exists but nothing will ever tell a
 * worker about it, and none in which Kafka has been told about a job that was rolled back.
 */
@Component
public class OutboxWriter {

    private final OutboxEventRepository outboxEventRepository;
    private final ObjectMapper objectMapper;

    public OutboxWriter(OutboxEventRepository outboxEventRepository, ObjectMapper objectMapper) {
        this.outboxEventRepository = outboxEventRepository;
        this.objectMapper = objectMapper;
    }

    public OutboxEventEntity append(String topic, JobEvent event) {
        String payload;
        try {
            payload = objectMapper.writeValueAsString(event);
        } catch (JsonProcessingException e) {
            // Would mean a broken event record; failing the transaction is correct here.
            throw new IllegalStateException("Could not serialise job event for the outbox", e);
        }

        OutboxEventEntity entity = new OutboxEventEntity(
                event.jobId(),
                event.eventType().name(),
                topic,
                payload,
                Instant.now());

        return outboxEventRepository.save(entity);
    }
}
