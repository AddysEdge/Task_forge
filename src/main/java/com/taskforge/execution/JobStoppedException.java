package com.taskforge.execution;

/**
 * Thrown by a handler (usually via {@link JobExecutionContext#checkStopRequested()}) to
 * unwind cleanly once cancellation or a timeout has been signalled.
 */
public class JobStoppedException extends RuntimeException {

    private final StopReason reason;

    public JobStoppedException(StopReason reason) {
        super("Job execution stopped: " + reason);
        this.reason = reason;
    }

    public StopReason getReason() {
        return reason;
    }
}
