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
 * Sleeps for a requested duration. Useful for demonstrating concurrency across workers,
 * cancellation of in-flight work, and timeout recovery.
 *
 * <p>Payload: {@code {"durationMs": 5000}}
 *
 * <p>The sleep is deliberately sliced rather than one long {@code Thread.sleep}: the slices
 * give the handler a place to observe the cooperative stop flag even on a platform where an
 * interrupt might be missed, and bound how long cancellation takes to be noticed.
 */
@Component
public class DelayJobHandler implements JobHandler {

    private static final Logger log = LoggerFactory.getLogger(DelayJobHandler.class);

    private static final int MIN_DURATION_MS = 1;
    private static final int MAX_DURATION_MS = 300_000;
    private static final long SLICE_MS = 100;

    @Override
    public JobType supportedType() {
        return JobType.DELAY;
    }

    @Override
    public void validatePayload(JsonNode payload) {
        PayloadFields.requireInt(payload, "durationMs", MIN_DURATION_MS, MAX_DURATION_MS);
    }

    @Override
    public JobExecutionResult execute(JobExecutionContext context) throws InterruptedException {
        int durationMs = PayloadFields.requireInt(
                context.payload(), "durationMs", MIN_DURATION_MS, MAX_DURATION_MS);

        log.debug("DELAY job {} sleeping for {}ms (attempt {})",
                context.jobId(), durationMs, context.attempt());

        long remaining = durationMs;
        long slept = 0;
        while (remaining > 0) {
            context.checkStopRequested();
            long chunk = Math.min(SLICE_MS, remaining);
            Thread.sleep(chunk);
            remaining -= chunk;
            slept += chunk;
        }
        context.checkStopRequested();

        ObjectNode result = JsonNodeFactory.instance.objectNode();
        result.put("sleptMs", slept);
        return JobExecutionResult.success(result);
    }
}
