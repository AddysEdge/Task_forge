package com.taskforge.execution;

import com.fasterxml.jackson.databind.JsonNode;
import com.taskforge.model.JobType;

import java.time.Instant;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Everything a handler is given about the attempt it is executing, plus the cooperative
 * stop signal.
 *
 * <p>TaskForge never forcibly kills a handler thread. Stopping is a two-part signal: this
 * object's {@link #stopReason} flag, and an interrupt delivered to the executing thread.
 * A well-behaved handler polls {@link #checkStopRequested()} inside its loops and lets
 * {@link InterruptedException} propagate from anything that blocks.
 */
public final class JobExecutionContext {

    private final UUID jobId;
    private final JobType jobType;
    private final JsonNode payload;
    private final int attempt;
    private final String workerId;
    private final Instant deadline;

    private final AtomicReference<StopReason> stopReason = new AtomicReference<>();
    private final AtomicReference<Thread> executingThread = new AtomicReference<>();

    public JobExecutionContext(UUID jobId,
                               JobType jobType,
                               JsonNode payload,
                               int attempt,
                               String workerId,
                               Instant deadline) {
        this.jobId = jobId;
        this.jobType = jobType;
        this.payload = payload;
        this.attempt = attempt;
        this.workerId = workerId;
        this.deadline = deadline;
    }

    public UUID jobId() {
        return jobId;
    }

    public JobType jobType() {
        return jobType;
    }

    public JsonNode payload() {
        return payload;
    }

    /** 1-based: attempt 1 is the original execution, attempt 2 is the first retry. */
    public int attempt() {
        return attempt;
    }

    public String workerId() {
        return workerId;
    }

    /** Wall-clock instant after which this attempt is considered timed out. */
    public Instant deadline() {
        return deadline;
    }

    /**
     * Called by the worker thread as it starts, so a stop signal knows whom to interrupt.
     * Not part of the handler-facing API.
     */
    public void bindExecutingThread(Thread thread) {
        executingThread.set(thread);
    }

    /**
     * Records the first stop reason and interrupts the handler thread.
     * Later calls are ignored, so a cancellation already in flight is not relabelled by a
     * subsequent timeout.
     *
     * @return true if this call was the one that set the reason
     */
    public boolean requestStop(StopReason reason) {
        boolean claimed = stopReason.compareAndSet(null, reason);
        Thread thread = executingThread.get();
        if (thread != null) {
            thread.interrupt();
        }
        return claimed;
    }

    public StopReason stopReason() {
        return stopReason.get();
    }

    public boolean isStopRequested() {
        return stopReason.get() != null;
    }

    /**
     * The cooperative checkpoint handlers should call inside long loops.
     *
     * @throws JobStoppedException if a stop was requested or the thread was interrupted
     */
    public void checkStopRequested() {
        StopReason reason = stopReason.get();
        if (reason != null) {
            throw new JobStoppedException(reason);
        }
        if (Thread.currentThread().isInterrupted()) {
            // Interrupted without a recorded reason: treat as cancellation.
            throw new JobStoppedException(StopReason.CANCELLED);
        }
    }
}
