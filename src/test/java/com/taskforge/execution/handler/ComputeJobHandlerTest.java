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

class ComputeJobHandlerTest {

    private final ComputeJobHandler handler = new ComputeJobHandler();
    private final ObjectMapper objectMapper = new ObjectMapper();

    private JobExecutionContext context(String payloadJson) throws Exception {
        return new JobExecutionContext(
                UUID.randomUUID(),
                JobType.COMPUTE,
                objectMapper.readTree(payloadJson),
                1,
                "test-worker",
                Instant.now().plusSeconds(300));
    }

    @Test
    void countsPrimesCorrectly() throws Exception {
        // 25 primes at or below 100 is a textbook value, so this checks the maths, not itself.
        JobExecutionResult result = handler.execute(context("{\"operation\":\"PRIME_COUNT\",\"n\":100}"));

        assertThat(result.isSuccess()).isTrue();
        assertThat(result.result().get("primeCount").asInt()).isEqualTo(25);
    }

    @Test
    void countsPrimesBelowTenThousand() throws Exception {
        JobExecutionResult result = handler.execute(context("{\"operation\":\"PRIME_COUNT\",\"n\":10000}"));
        assertThat(result.result().get("primeCount").asInt()).isEqualTo(1229);
    }

    @Test
    void computesFibonacci() throws Exception {
        JobExecutionResult result = handler.execute(context("{\"operation\":\"FIBONACCI\",\"n\":10}"));

        assertThat(result.isSuccess()).isTrue();
        assertThat(result.result().get("fibonacci").asText()).isEqualTo("55");
    }

    @Test
    @DisplayName("fibonacci stays exact well past the range of a long")
    void computesLargeFibonacciExactly() throws Exception {
        JobExecutionResult result = handler.execute(context("{\"operation\":\"FIBONACCI\",\"n\":300}"));

        assertThat(result.result().get("fibonacci").asText())
                .isEqualTo("222232244629420445529739893461909967206666939096499764990979600");
    }

    @Test
    void fibonacciOfZeroIsZero() throws Exception {
        JobExecutionResult result = handler.execute(context("{\"operation\":\"FIBONACCI\",\"n\":0}"));
        assertThat(result.result().get("fibonacci").asText()).isEqualTo("0");
    }

    @Test
    @DisplayName("the same checksum input always produces the same output")
    void checksumIsDeterministic() throws Exception {
        String payload = "{\"operation\":\"CHECKSUM\",\"input\":\"taskforge\",\"iterations\":1000}";

        String first = handler.execute(context(payload)).result().get("checksum").asText();
        String second = handler.execute(context(payload)).result().get("checksum").asText();

        assertThat(first).isEqualTo(second).hasSize(64);
    }

    @Test
    void differentChecksumInputsDiffer() throws Exception {
        String a = handler.execute(context("{\"operation\":\"CHECKSUM\",\"input\":\"a\",\"iterations\":10}"))
                .result().get("checksum").asText();
        String b = handler.execute(context("{\"operation\":\"CHECKSUM\",\"input\":\"b\",\"iterations\":10}"))
                .result().get("checksum").asText();

        assertThat(a).isNotEqualTo(b);
    }

    @Test
    void defaultsToPrimeCountWhenOperationOmitted() throws Exception {
        JobExecutionResult result = handler.execute(context("{\"n\":10}"));
        assertThat(result.result().get("operation").asText()).isEqualTo("PRIME_COUNT");
        assertThat(result.result().get("primeCount").asInt()).isEqualTo(4);
    }

    @Test
    @DisplayName("limits keep a single request from monopolising a worker")
    void rejectsInputsBeyondLimits() throws Exception {
        assertThatThrownBy(() -> handler.validatePayload(
                objectMapper.readTree("{\"operation\":\"PRIME_COUNT\",\"n\":5000001}")))
                .isInstanceOf(PayloadValidationException.class);

        assertThatThrownBy(() -> handler.validatePayload(
                objectMapper.readTree("{\"operation\":\"FIBONACCI\",\"n\":20001}")))
                .isInstanceOf(PayloadValidationException.class);

        assertThatThrownBy(() -> handler.validatePayload(
                objectMapper.readTree("{\"operation\":\"CHECKSUM\",\"iterations\":2000001}")))
                .isInstanceOf(PayloadValidationException.class);
    }

    @Test
    void rejectsUnknownOperation() throws Exception {
        assertThatThrownBy(() -> handler.validatePayload(
                objectMapper.readTree("{\"operation\":\"MINE_BITCOIN\",\"n\":10}")))
                .isInstanceOf(PayloadValidationException.class)
                .hasMessageContaining("operation");
    }

    @Test
    @DisplayName("a long CPU-bound computation still responds to cancellation")
    void respondsToCancellationMidComputation() throws Exception {
        // Two million SHA-256 rounds take seconds, so the cancellation below reliably lands
        // mid-computation rather than racing a job that already finished.
        JobExecutionContext context = context(
                "{\"operation\":\"CHECKSUM\",\"input\":\"taskforge\",\"iterations\":2000000}");
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
        Thread.sleep(100);
        context.requestStop(StopReason.CANCELLED);

        assertThat(finished.await(5, TimeUnit.SECONDS))
                .as("computation should abort shortly after the stop flag is set")
                .isTrue();
        assertThat(thrown.get()).isInstanceOf(JobStoppedException.class);
    }

    @Test
    void reportsSupportedType() {
        assertThat(handler.supportedType()).isEqualTo(JobType.COMPUTE);
    }
}
