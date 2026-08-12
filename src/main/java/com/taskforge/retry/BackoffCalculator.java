package com.taskforge.retry;

import com.taskforge.config.TaskForgeProperties;
import org.springframework.stereotype.Component;

import java.time.Duration;

/**
 * Exponential backoff: {@code initialDelay * multiplier^(retryNumber - 1)}, capped at
 * {@code maxDelay}.
 *
 * <p>With the defaults (1s, x2) retries land 1s, 2s, 4s, 8s after their failures.
 *
 * <p>The delay is turned into a persisted {@code next_attempt_at} timestamp rather than a
 * sleep, so nothing blocks a Kafka consumer thread and the schedule survives a restart of
 * every process in the system.
 *
 * <p>Deliberately deterministic — no jitter. That makes retry timing exactly assertable in
 * tests; at the scale where thundering herds matter, jitter belongs here.
 */
@Component
public class BackoffCalculator {

    private final TaskForgeProperties properties;

    public BackoffCalculator(TaskForgeProperties properties) {
        this.properties = properties;
    }

    /**
     * @param retryNumber 1 for the first retry after the original attempt failed
     */
    public Duration delayForRetry(int retryNumber) {
        if (retryNumber < 1) {
            throw new IllegalArgumentException("retryNumber must be at least 1, got " + retryNumber);
        }

        TaskForgeProperties.Retry retry = properties.getRetry();
        double factor = Math.pow(retry.getMultiplier(), retryNumber - 1.0);
        double millis = retry.getInitialDelay().toMillis() * factor;

        long capped = (long) Math.min(millis, (double) retry.getMaxDelay().toMillis());
        return Duration.ofMillis(Math.max(capped, 0));
    }
}
