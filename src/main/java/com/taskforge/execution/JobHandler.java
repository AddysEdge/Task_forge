package com.taskforge.execution;

import com.fasterxml.jackson.databind.JsonNode;
import com.taskforge.model.JobType;

/**
 * The extension point for job types.
 *
 * <p>Adding a new kind of work means writing one implementation and annotating it
 * {@code @Component}; {@link JobHandlerRegistry} discovers it and the worker loop is
 * untouched. There is no switch statement anywhere in the execution path.
 */
public interface JobHandler {

    JobType supportedType();

    /**
     * Runs one attempt. Implementations should poll
     * {@link JobExecutionContext#checkStopRequested()} inside loops so cancellation and
     * timeouts take effect promptly.
     *
     * <p>Throwing any exception other than {@link PermanentJobFailureException} marks the
     * attempt as transient and eligible for retry.
     */
    JobExecutionResult execute(JobExecutionContext context) throws Exception;

    /**
     * Rejects payloads this handler could never execute, at submission time rather than
     * after the job has been queued and dispatched.
     *
     * @throws com.taskforge.exception.PayloadValidationException if unusable
     */
    default void validatePayload(JsonNode payload) {
        // Handlers with no structural requirements accept anything.
    }
}
