package com.taskforge.model;

/**
 * The kinds of work TaskForge can execute. Each value is backed by exactly one
 * {@link com.taskforge.execution.JobHandler} discovered at startup.
 */
public enum JobType {

    /** Sleeps for a requested duration, responding to cooperative cancellation. */
    DELAY,

    /** Deterministic CPU-bound calculation with bounded inputs. */
    COMPUTE,

    /** Fails for the first N attempts, then succeeds. Used to demonstrate retries and the DLQ. */
    FAIL_TEST
}
