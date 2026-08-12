package com.taskforge.execution;

/**
 * Signals a failure that retrying cannot fix — a malformed payload, an unsupported
 * operation, a business rule violation.
 *
 * <p>Any other exception escaping a handler is treated as transient and retried. Making
 * "permanent" the explicit case keeps unexpected bugs from silently burning the retry
 * budget of a job that might well have succeeded on a second attempt.
 */
public class PermanentJobFailureException extends RuntimeException {

    public PermanentJobFailureException(String message) {
        super(message);
    }

    public PermanentJobFailureException(String message, Throwable cause) {
        super(message, cause);
    }
}
