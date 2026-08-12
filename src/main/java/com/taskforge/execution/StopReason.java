package com.taskforge.execution;

/** Why a running job was asked to stop. Determines the state it lands in afterwards. */
public enum StopReason {

    /** A client called the cancel endpoint. */
    CANCELLED,

    /** The job outlived its {@code timeoutSeconds} deadline. */
    TIMED_OUT,

    /**
     * This worker no longer owns the job — its lease lapsed and recovery reclaimed it.
     * The handler is stopped and, crucially, this worker writes no terminal state:
     * whatever picks the job up next is now responsible for it.
     */
    LEASE_LOST
}
