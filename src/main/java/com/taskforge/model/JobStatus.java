package com.taskforge.model;

import java.util.Set;

/**
 * Lifecycle states of a job. Transitions between them are owned exclusively by
 * {@link com.taskforge.service.JobStateMachine}; no other component may decide
 * that a given transition is legal.
 */
public enum JobStatus {

    /** Persisted and published to Kafka, waiting for a worker to claim it. */
    QUEUED,

    /** Claimed by exactly one worker, which holds a time-bounded lease on it. */
    RUNNING,

    /** A retryable attempt failed; waiting for its backoff window to elapse. */
    RETRYING,

    /** Finished successfully. Terminal. */
    COMPLETED,

    /** Failed in a way the handler declared non-retryable. Terminal. */
    FAILED,

    /** Cancellation was requested while the job was running; the worker is being signalled. */
    CANCEL_REQUESTED,

    /** Cancelled before or during execution. Terminal. */
    CANCELLED,

    /** Exceeded its timeout, or its lease expired and recovery reclaimed it. */
    TIMED_OUT,

    /** Exhausted its retry budget. Terminal, and published to the DLQ topic. */
    DEAD_LETTERED;

    private static final Set<JobStatus> TERMINAL =
            Set.of(COMPLETED, FAILED, CANCELLED, DEAD_LETTERED);

    /** Terminal states never return to execution, whatever arrives from Kafka afterwards. */
    public boolean isTerminal() {
        return TERMINAL.contains(this);
    }

    /** States in which a worker may still be actively holding the job. */
    public boolean isActive() {
        return this == RUNNING || this == CANCEL_REQUESTED;
    }
}
