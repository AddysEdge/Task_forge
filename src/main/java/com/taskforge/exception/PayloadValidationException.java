package com.taskforge.exception;

/** The job payload is syntactically valid JSON but not acceptable for the requested job type. */
public class PayloadValidationException extends RuntimeException {

    public PayloadValidationException(String message) {
        super(message);
    }
}
