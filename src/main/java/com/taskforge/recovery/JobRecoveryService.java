package com.taskforge.recovery;

import com.taskforge.cache.JobCache;
import com.taskforge.cache.WorkerHeartbeatStore;
import com.taskforge.config.TaskForgeProperties;
import com.taskforge.entity.JobEntity;
import com.taskforge.repository.JobRepository;
import com.taskforge.service.JobLifecycleService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.time.Instant;
import java.util.List;
import java.util.UUID;

/**
 * Reclaims jobs whose worker stopped looking after them.
 *
 * <p>A RUNNING row is suspicious for one of two reasons: its lease was not renewed, or it has
 * been running longer than its own timeout allows. Both are found by a single indexed scan
 * that locks candidates with {@code FOR UPDATE SKIP LOCKED} — which is what lets this run on
 * the API process and on every worker simultaneously without two of them recovering the same
 * job.
 *
 * <p>The Redis heartbeat refines the decision rather than driving it. If the owning worker is
 * still heartbeating, an expired lease is more likely a slow renewal than a dead process, so
 * the job gets a grace period before being taken away. If the heartbeat is gone, the job is
 * reclaimed immediately. The database lease remains the authority: Redis being wrong can only
 * change how quickly recovery acts, never whether it is entitled to.
 */
@Service
public class JobRecoveryService {

    private static final Logger log = LoggerFactory.getLogger(JobRecoveryService.class);

    private final JobRepository jobRepository;
    private final JobLifecycleService jobLifecycleService;
    private final WorkerHeartbeatStore heartbeatStore;
    private final JobCache jobCache;
    private final TaskForgeProperties properties;

    public JobRecoveryService(JobRepository jobRepository,
                              JobLifecycleService jobLifecycleService,
                              WorkerHeartbeatStore heartbeatStore,
                              JobCache jobCache,
                              TaskForgeProperties properties) {
        this.jobRepository = jobRepository;
        this.jobLifecycleService = jobLifecycleService;
        this.heartbeatStore = heartbeatStore;
        this.jobCache = jobCache;
        this.properties = properties;
    }

    /**
     * Runs one recovery pass.
     *
     * @return how many jobs were reclaimed
     */
    @Transactional
    public int recoverAbandonedJobs() {
        Instant now = Instant.now();
        List<UUID> candidates = jobRepository.lockAbandonedJobs(now, properties.getRecovery().getBatchSize());
        if (candidates.isEmpty()) {
            return 0;
        }

        int recovered = 0;
        for (UUID jobId : candidates) {
            JobEntity job = jobRepository.findById(jobId).orElse(null);
            if (job == null) {
                continue;
            }

            String reason = decideRecoveryReason(job, now);
            if (reason == null) {
                continue;
            }

            if (jobRepository.markTimedOutByRecovery(jobId, reason, now) == 0) {
                // Its worker finished it in the meantime. Nothing to do.
                continue;
            }

            jobCache.invalidate(jobId);
            log.warn("Job recovered jobId={} previousWorkerId={} attempt={} reason={}",
                    jobId, job.getWorkerId(), job.currentAttempt(), reason);

            // Same retry-or-dead-letter decision a locally detected timeout would make.
            jobLifecycleService.resolveTimedOut(job, reason, now);
            recovered++;
        }
        return recovered;
    }

    /**
     * @return why this job should be recovered, or null if it should be left alone for now
     */
    private String decideRecoveryReason(JobEntity job, Instant now) {
        if (exceededOwnTimeout(job, now)) {
            return "Execution exceeded its timeout of %d seconds and the owning worker did not act"
                    .formatted(job.getTimeoutSeconds());
        }

        Instant leaseExpiresAt = job.getLeaseExpiresAt();
        if (leaseExpiresAt == null || !leaseExpiresAt.isBefore(now)) {
            return null;
        }

        if (!heartbeatStore.isAlive(job.getWorkerId())) {
            return "Lease expired at %s and worker %s is no longer heartbeating"
                    .formatted(leaseExpiresAt, job.getWorkerId());
        }

        Instant graceEndsAt = leaseExpiresAt.plus(properties.getRecovery().getLiveWorkerGrace());
        if (now.isBefore(graceEndsAt)) {
            log.debug("Deferring recovery of jobId={}: worker {} is still alive, grace until {}",
                    job.getId(), job.getWorkerId(), graceEndsAt);
            return null;
        }

        return "Lease expired at %s and was not renewed within the grace period, despite worker %s reporting alive"
                .formatted(leaseExpiresAt, job.getWorkerId());
    }

    private boolean exceededOwnTimeout(JobEntity job, Instant now) {
        return job.getStartedAt() != null
                && job.getStartedAt().plusSeconds(job.getTimeoutSeconds()).isBefore(now);
    }
}
