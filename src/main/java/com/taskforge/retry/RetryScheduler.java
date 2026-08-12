package com.taskforge.retry;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

/**
 * Timer for {@link RetryRequeueService}. Separate bean so the transactional boundary is a
 * real proxy call and tests can trigger one deterministic pass.
 */
@Component
@ConditionalOnProperty(name = "taskforge.retry.enabled", havingValue = "true", matchIfMissing = true)
public class RetryScheduler {

    private static final Logger log = LoggerFactory.getLogger(RetryScheduler.class);

    private final RetryRequeueService retryRequeueService;

    public RetryScheduler(RetryRequeueService retryRequeueService) {
        this.retryRequeueService = retryRequeueService;
    }

    @Scheduled(fixedDelayString = "${taskforge.retry.scheduler-interval:1000}")
    public void requeueDueJobs() {
        try {
            retryRequeueService.requeueDueJobs();
        } catch (RuntimeException e) {
            // An escaping exception would cancel all future runs and leave every retrying
            // job stuck in backoff forever.
            log.error("Retry re-queue pass failed: {}", e.getMessage(), e);
        }
    }
}
