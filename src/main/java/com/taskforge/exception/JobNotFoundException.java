package com.taskforge.exception;

import java.util.UUID;

public class JobNotFoundException extends RuntimeException {

    private final UUID jobId;

    public JobNotFoundException(UUID jobId) {
        super("Job %s was not found".formatted(jobId));
        this.jobId = jobId;
    }

    public UUID getJobId() {
        return jobId;
    }
}
