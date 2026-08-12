package com.taskforge.worker;

import com.taskforge.config.WorkerIdentity;
import com.taskforge.execution.StopReason;
import com.taskforge.repository.JobRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.annotation.Profile;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

import java.util.UUID;

/**
 * Carries a cancellation request from the API process to the worker actually running the job.
 *
 * <p>The API cannot interrupt a thread in another container, so it writes CANCEL_REQUESTED to
 * the row and this poll turns that into a real signal on the executing thread. The database
 * is the message channel, which means the request survives a worker restart and needs no
 * additional infrastructure.
 */
@Component
@Profile({"worker", "all"})
public class CancellationPoller {

    private static final Logger log = LoggerFactory.getLogger(CancellationPoller.class);

    private final JobRepository jobRepository;
    private final RunningJobRegistry runningJobRegistry;
    private final WorkerIdentity workerIdentity;

    public CancellationPoller(JobRepository jobRepository,
                              RunningJobRegistry runningJobRegistry,
                              WorkerIdentity workerIdentity) {
        this.jobRepository = jobRepository;
        this.runningJobRegistry = runningJobRegistry;
        this.workerIdentity = workerIdentity;
    }

    @Scheduled(fixedDelayString = "${taskforge.worker.cancellation-poll-interval:2000}")
    @Transactional(readOnly = true)
    public void pollForCancellations() {
        if (runningJobRegistry.size() == 0) {
            return;
        }

        try {
            for (UUID jobId : jobRepository.findCancelRequestedForWorker(workerIdentity.workerId())) {
                runningJobRegistry.find(jobId).ifPresent(context -> {
                    if (context.requestStop(StopReason.CANCELLED)) {
                        log.info("Cancellation signalled to handler jobId={} workerId={}",
                                jobId, workerIdentity.workerId());
                    }
                });
            }
        } catch (RuntimeException e) {
            log.error("Cancellation poll failed for workerId={}: {}", workerIdentity.workerId(), e.getMessage());
        }
    }
}
