package com.taskforge.outbox;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

/**
 * Drives {@link OutboxPublisher} on a timer.
 *
 * <p>Kept separate from the publisher so that the transactional boundary is a real proxy
 * call rather than a self-invocation, and so tests can drive a single pass deterministically
 * without waiting on a scheduler.
 */
@Component
@ConditionalOnProperty(name = "taskforge.outbox.enabled", havingValue = "true", matchIfMissing = true)
public class OutboxPublisherScheduler {

    private static final Logger log = LoggerFactory.getLogger(OutboxPublisherScheduler.class);

    private final OutboxPublisher outboxPublisher;

    public OutboxPublisherScheduler(OutboxPublisher outboxPublisher) {
        this.outboxPublisher = outboxPublisher;
    }

    @Scheduled(fixedDelayString = "${taskforge.outbox.poll-interval:500}")
    public void publish() {
        try {
            outboxPublisher.publishPendingBatch();
        } catch (RuntimeException e) {
            // Never let a scheduled task die: a thrown exception would silently cancel all
            // future runs and strand every job still sitting in the outbox.
            log.error("Outbox publication pass failed: {}", e.getMessage(), e);
        }
    }
}
