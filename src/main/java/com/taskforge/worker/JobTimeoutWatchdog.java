package com.taskforge.worker;

import com.taskforge.execution.JobExecutionContext;
import com.taskforge.execution.StopReason;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.annotation.Profile;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.time.Instant;

/**
 * Enforces each job's own {@code timeoutSeconds} inside the worker that is running it.
 *
 * <p>This is the fast path for a job that simply takes too long: the worker is alive, notices
 * the deadline, stops the handler cooperatively and records TIMED_OUT itself. The recovery
 * scan is the slow path for the case this one cannot cover — a worker that is no longer
 * around to notice anything.
 */
@Component
@Profile({"worker", "all"})
public class JobTimeoutWatchdog {

    private static final Logger log = LoggerFactory.getLogger(JobTimeoutWatchdog.class);

    private final RunningJobRegistry runningJobRegistry;

    public JobTimeoutWatchdog(RunningJobRegistry runningJobRegistry) {
        this.runningJobRegistry = runningJobRegistry;
    }

    @Scheduled(fixedDelayString = "${taskforge.worker.timeout-check-interval:1000}")
    public void enforceDeadlines() {
        Instant now = Instant.now();

        for (JobExecutionContext context : runningJobRegistry.contexts()) {
            if (context.isStopRequested() || !now.isAfter(context.deadline())) {
                continue;
            }
            if (context.requestStop(StopReason.TIMED_OUT)) {
                log.warn("Timeout detected jobId={} workerId={} attempt={} deadline={}",
                        context.jobId(), context.workerId(), context.attempt(), context.deadline());
            }
        }
    }
}
