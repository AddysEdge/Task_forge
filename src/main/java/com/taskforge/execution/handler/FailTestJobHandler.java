package com.taskforge.execution.handler;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.taskforge.execution.JobExecutionContext;
import com.taskforge.execution.JobExecutionResult;
import com.taskforge.execution.JobHandler;
import com.taskforge.model.JobType;
import com.taskforge.validation.PayloadFields;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

/**
 * Fails on purpose for the first N attempts so retry, backoff and dead-lettering can be
 * demonstrated end to end without waiting for a real fault.
 *
 * <p>Payload: {@code {"failUntilAttempt": 2, "mode": "RETRYABLE"}}
 *
 * <p>With {@code failUntilAttempt: 2} attempts 1 and 2 fail and attempt 3 succeeds. Setting
 * it higher than {@code maxRetries} drives the job into the dead-letter path. {@code mode}
 * may be {@code RETRYABLE} (default) or {@code PERMANENT}; a permanent failure skips the
 * retry budget entirely and lands in FAILED.
 *
 * <p>Determinism matters here: the decision is a function of the persisted attempt number,
 * so a job behaves identically across restarts and across workers.
 */
@Component
public class FailTestJobHandler implements JobHandler {

    private static final Logger log = LoggerFactory.getLogger(FailTestJobHandler.class);

    public enum Mode {
        RETRYABLE,
        PERMANENT
    }

    private static final int MAX_FAIL_UNTIL_ATTEMPT = 100;

    @Override
    public JobType supportedType() {
        return JobType.FAIL_TEST;
    }

    @Override
    public void validatePayload(JsonNode payload) {
        PayloadFields.optionalInt(payload, "failUntilAttempt", 0, MAX_FAIL_UNTIL_ATTEMPT, 1);
        PayloadFields.optionalEnum(payload, "mode", Mode.class, Mode.RETRYABLE);
    }

    @Override
    public JobExecutionResult execute(JobExecutionContext context) {
        int failUntilAttempt = PayloadFields.optionalInt(
                context.payload(), "failUntilAttempt", 0, MAX_FAIL_UNTIL_ATTEMPT, 1);
        Mode mode = PayloadFields.optionalEnum(context.payload(), "mode", Mode.class, Mode.RETRYABLE);

        context.checkStopRequested();

        if (context.attempt() <= failUntilAttempt) {
            String message = "FAIL_TEST attempt %d of a configured %d failing attempts"
                    .formatted(context.attempt(), failUntilAttempt);
            log.info("jobId={} attempt={} deliberate {} failure", context.jobId(), context.attempt(), mode);
            return mode == Mode.PERMANENT
                    ? JobExecutionResult.permanentFailure(message)
                    : JobExecutionResult.retryableFailure(message);
        }

        ObjectNode result = JsonNodeFactory.instance.objectNode();
        result.put("succeededOnAttempt", context.attempt());
        result.put("failedAttempts", failUntilAttempt);
        return JobExecutionResult.success(result);
    }
}
