package com.taskforge.validation;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.taskforge.config.TaskForgeProperties;
import com.taskforge.exception.PayloadValidationException;
import com.taskforge.execution.JobHandlerRegistry;
import com.taskforge.execution.handler.ComputeJobHandler;
import com.taskforge.execution.handler.DelayJobHandler;
import com.taskforge.execution.handler.FailTestJobHandler;
import com.taskforge.model.JobType;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class JobPayloadValidatorTest {

    private final ObjectMapper objectMapper = new ObjectMapper();
    private TaskForgeProperties properties;
    private JobPayloadValidator validator;

    @BeforeEach
    void setUp() {
        properties = new TaskForgeProperties();
        JobHandlerRegistry registry = new JobHandlerRegistry(
                List.of(new DelayJobHandler(), new ComputeJobHandler(), new FailTestJobHandler()));
        validator = new JobPayloadValidator(registry, objectMapper, properties);
    }

    @Test
    void acceptsAndSerialisesAValidPayload() throws Exception {
        String json = validator.validateAndSerialize(
                JobType.DELAY, objectMapper.readTree("{\"durationMs\": 1000}"));

        assertThat(objectMapper.readTree(json).get("durationMs").asInt()).isEqualTo(1000);
    }

    @Test
    @DisplayName("validation is delegated to the handler that will run the job")
    void appliesHandlerSpecificRules() throws Exception {
        assertThatThrownBy(() -> validator.validateAndSerialize(
                JobType.DELAY, objectMapper.readTree("{\"wrongField\": 1}")))
                .isInstanceOf(PayloadValidationException.class)
                .hasMessageContaining("durationMs");

        // The same payload is fine for a job type that does not require that field.
        validator.validateAndSerialize(JobType.FAIL_TEST, objectMapper.readTree("{}"));
    }

    @Test
    void rejectsNonObjectPayloads() throws Exception {
        assertThatThrownBy(() -> validator.validateAndSerialize(
                JobType.DELAY, objectMapper.readTree("[1, 2, 3]")))
                .isInstanceOf(PayloadValidationException.class)
                .hasMessageContaining("JSON object");

        assertThatThrownBy(() -> validator.validateAndSerialize(
                JobType.DELAY, objectMapper.readTree("\"just a string\"")))
                .isInstanceOf(PayloadValidationException.class);
    }

    @Test
    @DisplayName("oversized payloads are rejected before anything is persisted")
    void enforcesPayloadSizeLimit() throws Exception {
        properties.getLimits().setMaxPayloadBytes(100);

        String oversized = "{\"durationMs\": 1000, \"note\": \"" + "x".repeat(200) + "\"}";

        assertThatThrownBy(() -> validator.validateAndSerialize(
                JobType.DELAY, objectMapper.readTree(oversized)))
                .isInstanceOf(PayloadValidationException.class)
                .hasMessageContaining("exceeds");
    }

    @Test
    void acceptsPayloadsAtTheSizeLimit() throws Exception {
        properties.getLimits().setMaxPayloadBytes(8192);
        validator.validateAndSerialize(JobType.DELAY, objectMapper.readTree("{\"durationMs\": 1}"));
    }
}
