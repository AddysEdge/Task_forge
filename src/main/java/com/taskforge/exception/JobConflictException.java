package com.taskforge.exception;

/**
 * The request was well formed but conflicts with the job's current state — for example
 * cancelling a job that already completed. Surfaces as HTTP 409.
 */
public class JobConflictException extends RuntimeException {

    public JobConflictException(String message) {
        super(message);
    }
}
