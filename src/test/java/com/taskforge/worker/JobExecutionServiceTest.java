package com.taskforge.worker;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.taskforge.config.TaskForgeProperties;
import com.taskforge.config.WorkerIdentity;
import com.taskforge.entity.JobEntity;
import com.taskforge.execution.JobExecutionContext;
import com.taskforge.execution.JobExecutionResult;
import com.taskforge.execution.JobHandler;
import com.taskforge.execution.JobHandlerRegistry;
import com.taskforge.execution.PermanentJobFailureException;
import com.taskforge.execution.StopReason;
import com.taskforge.model.JobType;
import com.taskforge.service.JobLifecycleService;
import com.taskforge.support.JobFixtures;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.junit.jupiter.MockitoSettings;
import org.mockito.quality.Strictness;

import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * The behaviour these tests protect is that receiving a message is not permission to run:
 * everything hinges on whether the atomic claim succeeded.
 */
@ExtendWith(MockitoExtension.class)
@MockitoSettings(strictness = Strictness.LENIENT)
class JobExecutionServiceTest {

    @Mock
    private JobLifecycleService jobLifecycleService;
    @Mock
    private JobHandlerRegistry handlerRegistry;

    private final RunningJobRegistry runningJobRegistry = new RunningJobRegistry();
    private final ObjectMapper objectMapper = new ObjectMapper();
    private WorkerIdentity workerIdentity;
    private JobExecutionService executionService;

    @BeforeEach
    void setUp() {
        TaskForgeProperties properties = new TaskForgeProperties();
        properties.getWorker().setId("test-worker");
        workerIdentity = new WorkerIdentity(properties);
        executionService = new JobExecutionService(
                jobLifecycleService, handlerRegistry, runningJobRegistry, workerIdentity, objectMapper);
    }

    private JobHandler handlerReturning(JobExecutionResult result) {
        return new JobHandler() {
            @Override
            public JobType supportedType() {
                return JobType.DELAY;
            }

            @Override
            public JobExecutionResult execute(JobExecutionContext context) {
                return result;
            }
        };
    }

    @Test
    @DisplayName("a duplicate delivery whose claim fails never reaches the handler")
    void doesNotExecuteWhenTheClaimIsLost() {
        UUID jobId = UUID.randomUUID();
        AtomicBoolean handlerRan = new AtomicBoolean(false);

        when(jobLifecycleService.claim(eq(jobId), anyString())).thenReturn(Optional.empty());
        when(jobLifecycleService.find(jobId)).thenReturn(Optional.empty());

        executionService.executeJob(jobId);

        assertThat(handlerRan).isFalse();
        verify(handlerRegistry, never()).handlerFor(any());
        verify(jobLifecycleService, never()).markCompleted(any(), anyString(), any(), anyLong());
        verify(jobLifecycleService, never()).markRetryableFailure(any(), anyString(), anyString());
    }

    @Test
    void recordsCompletionOnSuccess() {
        JobEntity job = JobFixtures.queuedDelay();
        when(jobLifecycleService.claim(eq(job.getId()), anyString())).thenReturn(Optional.of(job));
        when(handlerRegistry.handlerFor(JobType.DELAY))
                .thenReturn(handlerReturning(JobExecutionResult.success(
                        JsonNodeFactory.instance.objectNode().put("ok", true))));

        executionService.executeJob(job.getId());

        verify(jobLifecycleService).markCompleted(eq(job.getId()), eq("test-worker"), any(), anyLong());
    }

    @Test
    void recordsRetryableFailureWhenTheHandlerReportsOne() {
        JobEntity job = JobFixtures.queuedDelay();
        when(jobLifecycleService.claim(eq(job.getId()), anyString())).thenReturn(Optional.of(job));
        when(handlerRegistry.handlerFor(JobType.DELAY))
                .thenReturn(handlerReturning(JobExecutionResult.retryableFailure("temporary glitch")));

        executionService.executeJob(job.getId());

        verify(jobLifecycleService).markRetryableFailure(eq(job), eq("test-worker"), eq("temporary glitch"));
    }

    @Test
    void recordsPermanentFailureWhenTheHandlerReportsOne() {
        JobEntity job = JobFixtures.queuedDelay();
        when(jobLifecycleService.claim(eq(job.getId()), anyString())).thenReturn(Optional.of(job));
        when(handlerRegistry.handlerFor(JobType.DELAY))
                .thenReturn(handlerReturning(JobExecutionResult.permanentFailure("bad request")));

        executionService.executeJob(job.getId());

        verify(jobLifecycleService).markPermanentFailure(eq(job), eq("test-worker"), eq("bad request"));
    }

    @Test
    @DisplayName("an unexpected exception is treated as transient, so a bug does not burn the retry budget")
    void unexpectedExceptionsAreRetryable() {
        JobEntity job = JobFixtures.queuedDelay();
        when(jobLifecycleService.claim(eq(job.getId()), anyString())).thenReturn(Optional.of(job));
        when(handlerRegistry.handlerFor(JobType.DELAY)).thenReturn(new JobHandler() {
            @Override
            public JobType supportedType() {
                return JobType.DELAY;
            }

            @Override
            public JobExecutionResult execute(JobExecutionContext context) {
                throw new IllegalStateException("connection reset");
            }
        });

        executionService.executeJob(job.getId());

        verify(jobLifecycleService).markRetryableFailure(
                eq(job), eq("test-worker"), eq("IllegalStateException: connection reset"));
    }

    @Test
    void permanentFailureExceptionSkipsRetries() {
        JobEntity job = JobFixtures.queuedDelay();
        when(jobLifecycleService.claim(eq(job.getId()), anyString())).thenReturn(Optional.of(job));
        when(handlerRegistry.handlerFor(JobType.DELAY)).thenReturn(new JobHandler() {
            @Override
            public JobType supportedType() {
                return JobType.DELAY;
            }

            @Override
            public JobExecutionResult execute(JobExecutionContext context) {
                throw new PermanentJobFailureException("unsupported operation");
            }
        });

        executionService.executeJob(job.getId());

        verify(jobLifecycleService).markPermanentFailure(eq(job), eq("test-worker"), eq("unsupported operation"));
        verify(jobLifecycleService, never()).markRetryableFailure(any(), anyString(), anyString());
    }

    @Test
    @DisplayName("a handler that stops on a cancellation signal ends the job as cancelled")
    void cancellationSignalEndsTheJobAsCancelled() {
        JobEntity job = JobFixtures.queuedDelay();
        when(jobLifecycleService.claim(eq(job.getId()), anyString())).thenReturn(Optional.of(job));
        when(handlerRegistry.handlerFor(JobType.DELAY)).thenReturn(new JobHandler() {
            @Override
            public JobType supportedType() {
                return JobType.DELAY;
            }

            @Override
            public JobExecutionResult execute(JobExecutionContext context) {
                context.requestStop(StopReason.CANCELLED);
                context.checkStopRequested();
                return JobExecutionResult.success(null);
            }
        });

        executionService.executeJob(job.getId());

        verify(jobLifecycleService).markCancelled(job.getId(), "test-worker");
        verify(jobLifecycleService, never()).markCompleted(any(), anyString(), any(), anyLong());
    }

    @Test
    @DisplayName("a timeout signal routes to the timeout path, not to plain cancellation")
    void timeoutSignalEndsTheJobAsTimedOut() {
        JobEntity job = JobFixtures.queuedDelay();
        when(jobLifecycleService.claim(eq(job.getId()), anyString())).thenReturn(Optional.of(job));
        when(handlerRegistry.handlerFor(JobType.DELAY)).thenReturn(new JobHandler() {
            @Override
            public JobType supportedType() {
                return JobType.DELAY;
            }

            @Override
            public JobExecutionResult execute(JobExecutionContext context) {
                context.requestStop(StopReason.TIMED_OUT);
                context.checkStopRequested();
                return JobExecutionResult.success(null);
            }
        });

        executionService.executeJob(job.getId());

        verify(jobLifecycleService).markTimedOut(job, "test-worker");
        verify(jobLifecycleService, never()).markCancelled(any(), anyString());
    }

    @Test
    @DisplayName("losing the lease mid-flight leaves the job for whoever recovered it")
    void leaseLossWritesNoTerminalState() {
        JobEntity job = JobFixtures.queuedDelay();
        when(jobLifecycleService.claim(eq(job.getId()), anyString())).thenReturn(Optional.of(job));
        when(handlerRegistry.handlerFor(JobType.DELAY)).thenReturn(new JobHandler() {
            @Override
            public JobType supportedType() {
                return JobType.DELAY;
            }

            @Override
            public JobExecutionResult execute(JobExecutionContext context) {
                context.requestStop(StopReason.LEASE_LOST);
                context.checkStopRequested();
                return JobExecutionResult.success(null);
            }
        });

        executionService.executeJob(job.getId());

        verify(jobLifecycleService, never()).markCancelled(any(), anyString());
        verify(jobLifecycleService, never()).markTimedOut(any(), anyString());
        verify(jobLifecycleService, never()).markCompleted(any(), anyString(), any(), anyLong());
        verify(jobLifecycleService, never()).markRetryableFailure(any(), anyString(), anyString());
    }

    @Test
    void unreadableStoredPayloadFailsPermanentlyWithoutRunningTheHandler() {
        JobEntity job = JobEntity.builder()
                .idempotencyKey("broken")
                .jobType(JobType.DELAY)
                .payload("this is not json")
                .maxRetries(3)
                .timeoutSeconds(60)
                .build();

        when(jobLifecycleService.claim(eq(job.getId()), anyString())).thenReturn(Optional.of(job));

        executionService.executeJob(job.getId());

        verify(handlerRegistry, never()).handlerFor(any());
        verify(jobLifecycleService).markPermanentFailure(eq(job), eq("test-worker"), anyString());
    }

    @Test
    @DisplayName("the running registry is always cleaned up, whatever the outcome")
    void deregistersTheJobAfterwards() {
        JobEntity job = JobFixtures.queuedDelay();
        when(jobLifecycleService.claim(eq(job.getId()), anyString())).thenReturn(Optional.of(job));
        when(handlerRegistry.handlerFor(JobType.DELAY)).thenReturn(new JobHandler() {
            @Override
            public JobType supportedType() {
                return JobType.DELAY;
            }

            @Override
            public JobExecutionResult execute(JobExecutionContext context) {
                throw new IllegalStateException("boom");
            }
        });

        executionService.executeJob(job.getId());

        assertThat(runningJobRegistry.size()).isZero();
        assertThat(Thread.currentThread().isInterrupted())
                .as("the pooled thread must be left with a clean interrupt flag")
                .isFalse();
    }
}
