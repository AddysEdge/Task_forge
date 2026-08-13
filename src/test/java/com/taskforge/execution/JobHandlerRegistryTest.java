package com.taskforge.execution;

import com.taskforge.execution.handler.ComputeJobHandler;
import com.taskforge.execution.handler.DelayJobHandler;
import com.taskforge.execution.handler.FailTestJobHandler;
import com.taskforge.model.JobType;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class JobHandlerRegistryTest {

    private static JobHandler stubHandler(JobType type) {
        return new JobHandler() {
            @Override
            public JobType supportedType() {
                return type;
            }

            @Override
            public JobExecutionResult execute(JobExecutionContext context) {
                return JobExecutionResult.success(null);
            }
        };
    }

    @Test
    void resolvesEveryRegisteredType() {
        JobHandlerRegistry registry = new JobHandlerRegistry(
                List.of(new DelayJobHandler(), new ComputeJobHandler(), new FailTestJobHandler()));

        assertThat(registry.handlerFor(JobType.DELAY)).isInstanceOf(DelayJobHandler.class);
        assertThat(registry.handlerFor(JobType.COMPUTE)).isInstanceOf(ComputeJobHandler.class);
        assertThat(registry.handlerFor(JobType.FAIL_TEST)).isInstanceOf(FailTestJobHandler.class);
    }

    @Test
    @DisplayName("a job type with no handler fails at startup, not on the first job of that type")
    void rejectsMissingHandlerAtConstruction() {
        assertThatThrownBy(() -> new JobHandlerRegistry(List.of(new DelayJobHandler())))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("No JobHandler registered");
    }

    @Test
    @DisplayName("two handlers claiming one type is a configuration error, not a silent last-wins")
    void rejectsDuplicateHandlers() {
        assertThatThrownBy(() -> new JobHandlerRegistry(List.of(
                new DelayJobHandler(),
                new ComputeJobHandler(),
                new FailTestJobHandler(),
                stubHandler(JobType.DELAY))))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("claimed by both");
    }

    @Test
    void rejectsAnEmptyHandlerSet() {
        assertThatThrownBy(() -> new JobHandlerRegistry(List.of()))
                .isInstanceOf(IllegalStateException.class);
    }
}
