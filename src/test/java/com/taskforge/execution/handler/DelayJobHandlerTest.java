package com.taskforge.execution.handler;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.taskforge.exception.PayloadValidationException;
import com.taskforge.execution.JobExecutionContext;
import com.taskforge.execution.JobExecutionResult;
import com.taskforge.execution.JobStoppedException;
import com.taskforge.execution.StopReason;
import com.taskforge.model.JobType;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class DelayJobHandlerTest {

    private final DelayJobHandler handler = new DelayJobHandler();
    private final ObjectMapper objectMapper = new ObjectMapper();

    private JobExecutionContext context(String payloadJson) throws Exception {
        return new JobExecutionContext(
                UUID.randomUUID(),
                JobType.DELAY,
                objectMapper.readTree(payloadJson),
                1,
                "test-worker",
                Instant.now().plusSeconds(300));
    }

    @Test
    void sleepsForTheRequestedDuration() throws Exception {
        JobExecutionContext context = context("{\"durationMs\": 120}");

        long start = System.nanoTime();
        JobExecutionResult result = handler.execute(context);
        long elapsedMs = (System.nanoTime() - start) / 1_000_000;

        assertThat(result.isSuccess()).isTrue();
        assertThat(result.result().get("sleptMs").asLong()).isEqualTo(120);
        assertThat(elapsedMs).isGreaterThanOrEqualTo(100);
    }

    @Test
    @DisplayName("a cancellation signal stops the sleep promptly instead of running to completion")
    void respondsToCooperativeCancellation() throws Exception {
        JobExecutionContext context = context("{\"durationMs\": 30000}");
        AtomicReference<Throwable> thrown = new AtomicReference<>();
        CountDownLatch started = new CountDownLatch(1);
        CountDownLatch finished = new CountDownLatch(1);

        Thread worker = new Thread(() -> {
            context.bindExecutingThread(Thread.currentThread());
            started.countDown();
            try {
                handler.execute(context);
            } catch (Throwable t) {
                thrown.set(t);
            } finally {
                finished.countDown();
            }
        });
        worker.start();

        assertThat(started.await(2, TimeUnit.SECONDS)).isTrue();
        Thread.sleep(150);
        context.requestStop(StopReason.CANCELLED);

        // A 30s sleep must abort in well under a second once signalled.
        assertThat(finished.await(3, TimeUnit.SECONDS))
                .as("handler should unwind quickly after cancellation")
                .isTrue();
        assertThat(thrown.get()).isInstanceOfAny(JobStoppedException.class, InterruptedException.class);
    }

    @Test
    void rejectsMissingDuration() throws Exception {
        assertThatThrownBy(() -> handler.validatePayload(objectMapper.readTree("{}")))
                .isInstanceOf(PayloadValidationException.class)
                .hasMessageContaining("durationMs");
    }

    @Test
    void rejectsDurationOutsideBounds() throws Exception {
        assertThatThrownBy(() -> handler.validatePayload(objectMapper.readTree("{\"durationMs\": 0}")))
                .isInstanceOf(PayloadValidationException.class);
        assertThatThrownBy(() -> handler.validatePayload(objectMapper.readTree("{\"durationMs\": 300001}")))
                .isInstanceOf(PayloadValidationException.class);
    }

    @Test
    void rejectsNonNumericDuration() throws Exception {
        assertThatThrownBy(() -> handler.validatePayload(objectMapper.readTree("{\"durationMs\": \"soon\"}")))
                .isInstanceOf(PayloadValidationException.class);
    }

    @Test
    void acceptsValidPayload() throws Exception {
        handler.validatePayload(objectMapper.readTree("{\"durationMs\": 5000}"));
        assertThat(handler.supportedType()).isEqualTo(JobType.DELAY);
    }
}
