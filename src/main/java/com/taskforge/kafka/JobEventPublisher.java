package com.taskforge.kafka;

import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

import java.time.Duration;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * Thin synchronous wrapper over {@link KafkaTemplate}.
 *
 * <p>The outbox publisher needs to know whether the broker actually accepted a record before
 * it may mark the event published, so this deliberately blocks for the acknowledgement
 * rather than firing and forgetting. It is called from the outbox polling thread, never from
 * a request thread or a Kafka consumer thread.
 */
@Component
public class JobEventPublisher {

    private final KafkaTemplate<String, String> kafkaTemplate;

    public JobEventPublisher(KafkaTemplate<String, String> kafkaTemplate) {
        this.kafkaTemplate = kafkaTemplate;
    }

    /**
     * Sends a record and waits for the broker acknowledgement.
     *
     * @param key partition key; always the job id, so every event for one job keeps its order
     * @throws KafkaPublishException if the broker did not acknowledge within the timeout
     */
    public void publishAndWait(String topic, String key, String payload, Duration timeout) {
        try {
            kafkaTemplate.send(topic, key, payload).get(timeout.toMillis(), TimeUnit.MILLISECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new KafkaPublishException("Interrupted while publishing to " + topic, e);
        } catch (ExecutionException | TimeoutException e) {
            throw new KafkaPublishException("Failed to publish to " + topic, e);
        }
    }

    public static class KafkaPublishException extends RuntimeException {
        public KafkaPublishException(String message, Throwable cause) {
            super(message, cause);
        }
    }
}
