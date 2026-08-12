package com.taskforge.outbox;

import com.taskforge.config.TaskForgeProperties;
import com.taskforge.entity.OutboxEventEntity;
import com.taskforge.kafka.JobEventPublisher;
import com.taskforge.repository.OutboxEventRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.time.Instant;
import java.util.List;

/**
 * Drains the outbox to Kafka.
 *
 * <p>Delivery is deliberately at-least-once. The publisher sends the record, waits for the
 * broker acknowledgement, then marks the row PUBLISHED — and if the process dies between
 * those two steps the row is still PENDING and will be sent again on the next pass. That
 * duplicate is not a defect to be engineered away here; it is paid for once, cheaply, by
 * making workers idempotent (see {@code JobExecutionService}).
 *
 * <p>A failing send leaves the row PENDING with an incremented attempt count, so a Kafka
 * outage delays jobs but never strands them.
 */
@Service
public class OutboxPublisher {

    private static final Logger log = LoggerFactory.getLogger(OutboxPublisher.class);
    private static final int MAX_ERROR_LENGTH = 1000;

    private final OutboxEventRepository outboxEventRepository;
    private final JobEventPublisher jobEventPublisher;
    private final TaskForgeProperties properties;

    public OutboxPublisher(OutboxEventRepository outboxEventRepository,
                           JobEventPublisher jobEventPublisher,
                           TaskForgeProperties properties) {
        this.outboxEventRepository = outboxEventRepository;
        this.jobEventPublisher = jobEventPublisher;
        this.properties = properties;
    }

    /**
     * Publishes one batch of pending events.
     *
     * <p>Runs in a transaction so the {@code FOR UPDATE SKIP LOCKED} row locks taken by
     * {@link OutboxEventRepository#lockPendingBatch(int)} are held for the whole pass. Every
     * instance of the application can run this concurrently; each row goes to exactly one.
     *
     * @return the number of events successfully published
     */
    @Transactional
    public int publishPendingBatch() {
        List<OutboxEventEntity> pending =
                outboxEventRepository.lockPendingBatch(properties.getOutbox().getBatchSize());
        if (pending.isEmpty()) {
            return 0;
        }

        int published = 0;
        for (OutboxEventEntity event : pending) {
            if (publish(event)) {
                published++;
            }
        }

        log.debug("Outbox pass published {} of {} locked events", published, pending.size());
        return published;
    }

    private boolean publish(OutboxEventEntity event) {
        try {
            jobEventPublisher.publishAndWait(
                    event.getTopic(),
                    event.getAggregateId().toString(),
                    event.getPayload(),
                    properties.getOutbox().getPublishTimeout());

            outboxEventRepository.markPublished(event.getId(), Instant.now());
            log.info("Job published eventType={} jobId={} topic={} outboxEventId={}",
                    event.getEventType(), event.getAggregateId(), event.getTopic(), event.getId());
            return true;
        } catch (RuntimeException e) {
            // Left PENDING on purpose: the next pass retries it. Recorded, not swallowed.
            outboxEventRepository.recordPublishFailure(event.getId(), truncate(e.getMessage()));
            log.error("Outbox publication failed jobId={} topic={} outboxEventId={} attempt={}: {}",
                    event.getAggregateId(), event.getTopic(), event.getId(),
                    event.getAttemptCount() + 1, e.getMessage());
            return false;
        }
    }

    private String truncate(String message) {
        if (message == null) {
            return "unknown error";
        }
        return message.length() <= MAX_ERROR_LENGTH ? message : message.substring(0, MAX_ERROR_LENGTH);
    }
}
