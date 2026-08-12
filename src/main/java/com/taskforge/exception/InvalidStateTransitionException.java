package com.taskforge.exception;

import com.taskforge.model.JobStatus;

/** Raised when something attempts a job state change the lifecycle does not permit. */
public class InvalidStateTransitionException extends RuntimeException {

    private final JobStatus from;
    private final JobStatus to;

    public InvalidStateTransitionException(JobStatus from, JobStatus to) {
        super("Illegal job state transition %s -> %s".formatted(from, to));
        this.from = from;
        this.to = to;
    }

    public JobStatus getFrom() {
        return from;
    }

    public JobStatus getTo() {
        return to;
    }
}
