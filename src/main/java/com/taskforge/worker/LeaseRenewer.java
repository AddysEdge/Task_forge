package com.taskforge.worker;

import com.taskforge.config.WorkerIdentity;
import com.taskforge.execution.StopReason;
import com.taskforge.service.JobLifecycleService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.annotation.Profile;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.util.UUID;

/**
 * Keeps the database lease alive for jobs this worker is executing.
 *
 * <p>A lease is a promise that expires. Long jobs stay owned because this renews them; a
 * worker that dies stops renewing and its jobs become recoverable without anyone needing to
 * detect the death directly.
 *
 * <p>If a renewal matches no rows, this worker no longer owns the job — recovery got there
 * first. The handler is then signalled to stop with {@link StopReason#LEASE_LOST}, which is
 * what prevents two processes from both believing they are executing the same job.
 */
@Component
@Profile({"worker", "all"})
public class LeaseRenewer {

    private static final Logger log = LoggerFactory.getLogger(LeaseRenewer.class);

    private final RunningJobRegistry runningJobRegistry;
    private final JobLifecycleService jobLifecycleService;
    private final WorkerIdentity workerIdentity;

    public LeaseRenewer(RunningJobRegistry runningJobRegistry,
                        JobLifecycleService jobLifecycleService,
                        WorkerIdentity workerIdentity) {
        this.runningJobRegistry = runningJobRegistry;
        this.jobLifecycleService = jobLifecycleService;
        this.workerIdentity = workerIdentity;
    }

    @Scheduled(fixedDelayString = "${taskforge.worker.lease-renewal-interval:15000}")
    public void renewLeases() {
        String workerId = workerIdentity.workerId();

        for (UUID jobId : runningJobRegistry.runningJobIds()) {
            try {
                if (jobLifecycleService.renewLease(jobId, workerId)) {
                    continue;
                }
                log.warn("Lease lost jobId={} workerId={}; signalling the handler to stop", jobId, workerId);
                runningJobRegistry.find(jobId)
                        .ifPresent(context -> context.requestStop(StopReason.LEASE_LOST));
            } catch (RuntimeException e) {
                // A transient database problem must not kill the renewal loop; the lease
                // simply has to survive until the next pass.
                log.error("Lease renewal failed jobId={} workerId={}: {}", jobId, workerId, e.getMessage());
            }
        }
    }
}
