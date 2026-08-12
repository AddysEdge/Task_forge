package com.taskforge.execution;

import com.fasterxml.jackson.databind.JsonNode;

/**
 * Outcome of a single execution attempt.
 *
 * <p>The distinction between {@link Outcome#RETRYABLE_FAILURE} and
 * {@link Outcome#PERMANENT_FAILURE} is the one the retry engine acts on: only the former
 * consumes a retry and comes back later.
 */
public record JobExecutionResult(Outcome outcome, JsonNode result, String message) {

    public enum Outcome {
        SUCCESS,
        RETRYABLE_FAILURE,
        PERMANENT_FAILURE
    }

    public static JobExecutionResult success(JsonNode result) {
        return new JobExecutionResult(Outcome.SUCCESS, result, null);
    }

    public static JobExecutionResult retryableFailure(String message) {
        return new JobExecutionResult(Outcome.RETRYABLE_FAILURE, null, message);
    }

    public static JobExecutionResult permanentFailure(String message) {
        return new JobExecutionResult(Outcome.PERMANENT_FAILURE, null, message);
    }

    public boolean isSuccess() {
        return outcome == Outcome.SUCCESS;
    }
}
