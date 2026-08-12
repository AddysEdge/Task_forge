package com.taskforge.execution.handler;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.taskforge.execution.JobExecutionContext;
import com.taskforge.execution.JobExecutionResult;
import com.taskforge.execution.JobHandler;
import com.taskforge.execution.PermanentJobFailureException;
import com.taskforge.model.JobType;
import com.taskforge.validation.PayloadFields;
import org.springframework.stereotype.Component;

import java.math.BigInteger;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.HexFormat;

/**
 * Deterministic CPU-bound work, used to show real parallelism across worker threads and
 * processes.
 *
 * <p>Payloads:
 * <ul>
 *   <li>{@code {"operation": "PRIME_COUNT", "n": 1000000}} — primes less than or equal to n</li>
 *   <li>{@code {"operation": "FIBONACCI", "n": 10000}} — exact Fibonacci number</li>
 *   <li>{@code {"operation": "CHECKSUM", "input": "abc", "iterations": 100000}} — iterated SHA-256</li>
 * </ul>
 *
 * <p>Every bound is small enough that a single request cannot monopolise a worker: the
 * limits below cap each operation at roughly a second of CPU on ordinary hardware.
 */
@Component
public class ComputeJobHandler implements JobHandler {

    public enum Operation {
        PRIME_COUNT,
        FIBONACCI,
        CHECKSUM
    }

    private static final int MAX_PRIME_N = 5_000_000;
    private static final int MAX_FIBONACCI_N = 20_000;
    private static final int MAX_CHECKSUM_ITERATIONS = 2_000_000;
    private static final int MAX_CHECKSUM_INPUT_LENGTH = 1024;

    /** How often CPU loops check for cancellation. Small enough to be responsive, large enough to be free. */
    private static final int STOP_CHECK_INTERVAL = 8192;

    @Override
    public JobType supportedType() {
        return JobType.COMPUTE;
    }

    @Override
    public void validatePayload(JsonNode payload) {
        Operation operation = PayloadFields.optionalEnum(payload, "operation", Operation.class, Operation.PRIME_COUNT);
        switch (operation) {
            case PRIME_COUNT -> PayloadFields.requireInt(payload, "n", 1, MAX_PRIME_N);
            case FIBONACCI -> PayloadFields.requireInt(payload, "n", 0, MAX_FIBONACCI_N);
            case CHECKSUM -> {
                PayloadFields.optionalText(payload, "input", MAX_CHECKSUM_INPUT_LENGTH, "taskforge");
                PayloadFields.requireInt(payload, "iterations", 1, MAX_CHECKSUM_ITERATIONS);
            }
        }
    }

    @Override
    public JobExecutionResult execute(JobExecutionContext context) {
        JsonNode payload = context.payload();
        Operation operation = PayloadFields.optionalEnum(payload, "operation", Operation.class, Operation.PRIME_COUNT);

        long startedAt = System.nanoTime();
        ObjectNode result = JsonNodeFactory.instance.objectNode();
        result.put("operation", operation.name());

        switch (operation) {
            case PRIME_COUNT -> {
                int n = PayloadFields.requireInt(payload, "n", 1, MAX_PRIME_N);
                result.put("n", n);
                result.put("primeCount", countPrimes(n, context));
            }
            case FIBONACCI -> {
                int n = PayloadFields.requireInt(payload, "n", 0, MAX_FIBONACCI_N);
                result.put("n", n);
                result.put("fibonacci", fibonacci(n, context).toString());
            }
            case CHECKSUM -> {
                String input = PayloadFields.optionalText(payload, "input", MAX_CHECKSUM_INPUT_LENGTH, "taskforge");
                int iterations = PayloadFields.requireInt(payload, "iterations", 1, MAX_CHECKSUM_ITERATIONS);
                result.put("iterations", iterations);
                result.put("checksum", iteratedSha256(input, iterations, context));
            }
        }

        result.put("elapsedMs", (System.nanoTime() - startedAt) / 1_000_000);
        return JobExecutionResult.success(result);
    }

    /** Sieve of Eratosthenes. */
    private int countPrimes(int n, JobExecutionContext context) {
        boolean[] composite = new boolean[n + 1];
        int count = 0;
        for (int candidate = 2; candidate <= n; candidate++) {
            if (candidate % STOP_CHECK_INTERVAL == 0) {
                context.checkStopRequested();
            }
            if (composite[candidate]) {
                continue;
            }
            count++;
            for (long multiple = (long) candidate * candidate; multiple <= n; multiple += candidate) {
                composite[(int) multiple] = true;
            }
        }
        return count;
    }

    /** Iterative so that large n cannot blow the stack. */
    private BigInteger fibonacci(int n, JobExecutionContext context) {
        BigInteger previous = BigInteger.ZERO;
        BigInteger current = BigInteger.ONE;
        for (int i = 0; i < n; i++) {
            if (i % STOP_CHECK_INTERVAL == 0) {
                context.checkStopRequested();
            }
            BigInteger next = previous.add(current);
            previous = current;
            current = next;
        }
        return previous;
    }

    private String iteratedSha256(String input, int iterations, JobExecutionContext context) {
        MessageDigest digest;
        try {
            digest = MessageDigest.getInstance("SHA-256");
        } catch (NoSuchAlgorithmException e) {
            // SHA-256 is mandated by the platform; if it is missing, retrying will not help.
            throw new PermanentJobFailureException("SHA-256 is unavailable on this JVM", e);
        }

        byte[] value = input.getBytes(StandardCharsets.UTF_8);
        for (int i = 0; i < iterations; i++) {
            if (i % STOP_CHECK_INTERVAL == 0) {
                context.checkStopRequested();
            }
            value = digest.digest(value);
        }
        return HexFormat.of().formatHex(value);
    }
}
