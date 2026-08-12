package com.taskforge.kafka;

public enum JobEventType {

    /** A newly accepted job is ready for a worker. Published to the main jobs topic. */
    JOB_SUBMITTED,

    /** A previously failed job whose backoff has elapsed. Published to the retry topic. */
    JOB_RETRY_SCHEDULED,

    /** A job that exhausted its retry budget. Published to the DLQ topic; never consumed by workers. */
    JOB_DEAD_LETTERED
}
