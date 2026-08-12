package com.taskforge.recovery;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

/**
 * Timer for {@link JobRecoveryService}. Every instance runs it; {@code SKIP LOCKED} makes
 * that safe, and it means recovery does not depend on any one process being alive.
 */
@Component
@ConditionalOnProperty(name = "taskforge.recovery.enabled", havingValue = "true", matchIfMissing = true)
public class RecoveryScheduler {

    private static final Logger log = LoggerFactory.getLogger(RecoveryScheduler.class);

    private final JobRecoveryService jobRecoveryService;

    public RecoveryScheduler(JobRecoveryService jobRecoveryService) {
        this.jobRecoveryService = jobRecoveryService;
    }

    @Scheduled(fixedDelayString = "${taskforge.recovery.interval:10000}")
    public void recover() {
        try {
            jobRecoveryService.recoverAbandonedJobs();
        } catch (RuntimeException e) {
            log.error("Recovery pass failed: {}", e.getMessage(), e);
        }
    }
}
