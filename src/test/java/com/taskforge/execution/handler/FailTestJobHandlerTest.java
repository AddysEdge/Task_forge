package com.taskforge.execution.handler;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.taskforge.exception.PayloadValidationException;
import com.taskforge.execution.JobExecutionContext;
import com.taskforge.execution.JobExecutionResult;
import com.taskforge.model.JobType;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class FailTestJobHandlerTest {

    private final FailTestJobHandler handler = new FailTestJobHandler();
    private final ObjectMapper objectMapper = new ObjectMapper();

    private JobExecutionContext context(String payloadJson, int attempt) throws Exception {
        return new JobExecutionContext(
                UUID.randomUUID(),
                JobType.FAIL_TEST,
                objectMapper.readTree(payloadJson),
                attempt,
                "test-worker",
                Instant.now().plusSeconds(300));
    }

    @Test
    @DisplayName("failUntilAttempt=2 fails attempts 1 and 2, then succeeds on attempt 3")
    void failsUntilConfiguredAttempt() throws Exception {
        String payload = "{\"failUntilAttempt\": 2}";

        assertThat(handler.execute(context(payload, 1)).outcome())
                .isEqualTo(JobExecutionResult.Outcome.RETRYABLE_FAILURE);
        assertThat(handler.execute(context(payload, 2)).outcome())
                .isEqualTo(JobExecutionResult.Outcome.RETRYABLE_FAILURE);

        JobExecutionResult third = handler.execute(context(payload, 3));
        assertThat(third.isSuccess()).isTrue();
        assertThat(third.result().get("succeededOnAttempt").asInt()).isEqualTo(3);
    }

    @Test
    @DisplayName("the outcome depends only on the persisted attempt number, so it survives restarts")
    void outcomeIsDeterministicForAGivenAttempt() throws Exception {
        String payload = "{\"failUntilAttempt\": 1}";

        for (int i = 0; i < 5; i++) {
            assertThat(handler.execute(context(payload, 1)).outcome())
                    .isEqualTo(JobExecutionResult.Outcome.RETRYABLE_FAILURE);
            assertThat(handler.execute(context(payload, 2)).isSuccess()).isTrue();
        }
    }

    @Test
    void failUntilAttemptZeroSucceedsImmediately() throws Exception {
        assertThat(handler.execute(context("{\"failUntilAttempt\": 0}", 1)).isSuccess()).isTrue();
    }

    @Test
    @DisplayName("PERMANENT mode skips the retry budget entirely")
    void permanentModeReportsPermanentFailure() throws Exception {
        JobExecutionResult result = handler.execute(
                context("{\"failUntilAttempt\": 5, \"mode\": \"PERMANENT\"}", 1));

        assertThat(result.outcome()).isEqualTo(JobExecutionResult.Outcome.PERMANENT_FAILURE);
        assertThat(result.message()).contains("attempt 1");
    }

    @Test
    void defaultsToFailingTheFirstAttemptOnly() throws Exception {
        assertThat(handler.execute(context("{}", 1)).outcome())
                .isEqualTo(JobExecutionResult.Outcome.RETRYABLE_FAILURE);
        assertThat(handler.execute(context("{}", 2)).isSuccess()).isTrue();
    }

    @Test
    void rejectsInvalidPayloads() throws Exception {
        assertThatThrownBy(() -> handler.validatePayload(
                objectMapper.readTree("{\"failUntilAttempt\": 101}")))
                .isInstanceOf(PayloadValidationException.class);

        assertThatThrownBy(() -> handler.validatePayload(
                objectMapper.readTree("{\"mode\": \"SOMETIMES\"}")))
                .isInstanceOf(PayloadValidationException.class);
    }

    @Test
    void reportsSupportedType() {
        assertThat(handler.supportedType()).isEqualTo(JobType.FAIL_TEST);
    }
}
