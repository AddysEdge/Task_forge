package com.taskforge.it;

import com.taskforge.dto.ApiError;
import com.taskforge.dto.CreateJobRequest;
import com.taskforge.dto.JobResponse;
import com.taskforge.entity.JobEntity;
import com.taskforge.model.JobStatus;
import com.taskforge.model.JobType;
import com.taskforge.support.AbstractIntegrationTest;
import com.taskforge.support.JobFixtures;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpMethod;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

/** REST surface: submission, validation, idempotency, reads and cancellation. */
class JobApiIT extends AbstractIntegrationTest {

    // ------------------------------------------------------------------
    // Submission
    // ------------------------------------------------------------------

    @Test
    void submitsAJobAndReturnsItsLocation() {
        CreateJobRequest request = newRequest(JobType.DELAY, "{\"durationMs\": 50}", 3, 60);

        ResponseEntity<JobResponse> response = submit(request);

        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.CREATED);
        assertThat(response.getHeaders().getLocation()).isNotNull();
        JobResponse job = response.getBody();
        assertThat(job).isNotNull();
        assertThat(job.jobType()).isEqualTo(JobType.DELAY);
        assertThat(job.idempotencyKey()).isEqualTo(request.idempotencyKey());
        assertThat(job.maxRetries()).isEqualTo(3);
        assertThat(job.timeoutSeconds()).isEqualTo(60);
        assertThat(job.attempt()).isEqualTo(1);
        assertThat(job.retryCount()).isZero();
    }

    @Test
    @DisplayName("omitted maxRetries and timeoutSeconds fall back to configured defaults")
    void appliesDefaultsForOptionalFields() throws Exception {
        CreateJobRequest request = new CreateJobRequest(
                "defaults-" + UUID.randomUUID(),
                JobType.FAIL_TEST,
                objectMapper.readTree("{\"failUntilAttempt\": 0}"),
                null,
                null);

        JobResponse job = submitSuccessfully(request);

        assertThat(job.maxRetries()).isEqualTo(3);
        assertThat(job.timeoutSeconds()).isEqualTo(60);
    }

    // ------------------------------------------------------------------
    // Idempotency
    // ------------------------------------------------------------------

    @Test
    @DisplayName("re-submitting the same idempotency key returns 200 and the original job")
    void repeatedSubmissionIsDeduplicated() {
        CreateJobRequest request = newRequest(JobType.DELAY, "{\"durationMs\": 50}", 3, 60);

        ResponseEntity<JobResponse> first = submit(request);
        ResponseEntity<JobResponse> second = submit(request);
        ResponseEntity<JobResponse> third = submit(request);

        assertThat(first.getStatusCode()).isEqualTo(HttpStatus.CREATED);
        assertThat(second.getStatusCode()).isEqualTo(HttpStatus.OK);
        assertThat(third.getStatusCode()).isEqualTo(HttpStatus.OK);

        UUID jobId = first.getBody().id();
        assertThat(second.getBody().id()).isEqualTo(jobId);
        assertThat(third.getBody().id()).isEqualTo(jobId);

        assertThat(jobRepository.findByIdempotencyKey(request.idempotencyKey())).isPresent();
    }

    @Test
    @DisplayName("16 simultaneous requests with one idempotency key create exactly one job")
    void concurrentSubmissionsCollapseToOneJob() throws Exception {
        CreateJobRequest request = newRequest(JobType.DELAY, "{\"durationMs\": 50}", 3, 60);
        int concurrency = 16;

        ExecutorService pool = Executors.newFixedThreadPool(concurrency);
        CountDownLatch startGun = new CountDownLatch(1);
        List<Callable<ResponseEntity<JobResponse>>> calls = new ArrayList<>();
        for (int i = 0; i < concurrency; i++) {
            calls.add(() -> {
                startGun.await(5, TimeUnit.SECONDS);
                return submit(request);
            });
        }

        List<Future<ResponseEntity<JobResponse>>> futures = new ArrayList<>();
        for (Callable<ResponseEntity<JobResponse>> call : calls) {
            futures.add(pool.submit(call));
        }
        startGun.countDown();

        List<ResponseEntity<JobResponse>> responses = new ArrayList<>();
        for (Future<ResponseEntity<JobResponse>> future : futures) {
            responses.add(future.get(30, TimeUnit.SECONDS));
        }
        pool.shutdownNow();

        long created = responses.stream()
                .filter(r -> r.getStatusCode() == HttpStatus.CREATED)
                .count();
        assertThat(created)
                .as("exactly one request may win the unique index")
                .isEqualTo(1);

        List<UUID> distinctIds = responses.stream()
                .map(r -> r.getBody().id())
                .distinct()
                .toList();
        assertThat(distinctIds)
                .as("every caller must be told about the same job")
                .hasSize(1);

        // And the database agrees: one row, one outbox event, no duplicate work queued.
        assertThat(jobRepository.findByIdempotencyKey(request.idempotencyKey())).isPresent();
        assertThat(outboxEventRepository.findByAggregateIdOrderByCreatedAt(distinctIds.get(0))).hasSize(1);
    }

    // ------------------------------------------------------------------
    // Reads
    // ------------------------------------------------------------------

    @Test
    void retrievesASubmittedJob() {
        JobResponse submitted = submitSuccessfully(newRequest(JobType.DELAY, "{\"durationMs\": 50}", 3, 60));

        ResponseEntity<JobResponse> response =
                rest.getForEntity("/api/v1/jobs/{id}", JobResponse.class, submitted.id());

        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.OK);
        assertThat(response.getBody().id()).isEqualTo(submitted.id());
        assertThat(response.getBody().payload().get("durationMs").asInt()).isEqualTo(50);
    }

    @Test
    void unknownJobReturns404() {
        ResponseEntity<ApiError> response =
                rest.getForEntity("/api/v1/jobs/{id}", ApiError.class, UUID.randomUUID());

        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.NOT_FOUND);
        assertThat(response.getBody().error()).isEqualTo("JOB_NOT_FOUND");
        assertThat(response.getBody().message()).doesNotContain("Exception");
    }

    @Test
    void listsJobsNewestFirstWithPagination() {
        submitSuccessfully(newRequest(JobType.DELAY, "{\"durationMs\": 50}", 3, 60));
        submitSuccessfully(newRequest(JobType.DELAY, "{\"durationMs\": 50}", 3, 60));

        ResponseEntity<String> response = rest.getForEntity("/api/v1/jobs?page=0&size=2", String.class);

        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.OK);
        assertThat(response.getBody()).contains("\"content\"", "\"totalElements\"", "\"page\":0", "\"size\":2");
    }

    @Test
    void filtersJobsByStatus() {
        ResponseEntity<String> response =
                rest.getForEntity("/api/v1/jobs?status=DEAD_LETTERED&size=5", String.class);

        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.OK);
    }

    @Test
    void rejectsAnUnknownStatusFilter() {
        ResponseEntity<ApiError> response =
                rest.getForEntity("/api/v1/jobs?status=NOT_A_STATUS", ApiError.class);

        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.BAD_REQUEST);
        assertThat(response.getBody().error()).isEqualTo("INVALID_PARAMETER");
    }

    // ------------------------------------------------------------------
    // Validation
    // ------------------------------------------------------------------

    @Test
    void rejectsABlankIdempotencyKey() throws Exception {
        CreateJobRequest request = new CreateJobRequest(
                "  ", JobType.DELAY, objectMapper.readTree("{\"durationMs\": 50}"), 3, 60);

        ResponseEntity<ApiError> response = rest.postForEntity("/api/v1/jobs", request, ApiError.class);

        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.BAD_REQUEST);
        assertThat(response.getBody().error()).isEqualTo("VALIDATION_ERROR");
    }

    @Test
    void rejectsOutOfRangeRetryAndTimeoutLimits() throws Exception {
        CreateJobRequest tooManyRetries = new CreateJobRequest(
                "k1-" + UUID.randomUUID(), JobType.DELAY,
                objectMapper.readTree("{\"durationMs\": 50}"), 11, 60);
        CreateJobRequest badTimeout = new CreateJobRequest(
                "k2-" + UUID.randomUUID(), JobType.DELAY,
                objectMapper.readTree("{\"durationMs\": 50}"), 3, 0);
        CreateJobRequest hugeTimeout = new CreateJobRequest(
                "k3-" + UUID.randomUUID(), JobType.DELAY,
                objectMapper.readTree("{\"durationMs\": 50}"), 3, 99999);

        assertThat(rest.postForEntity("/api/v1/jobs", tooManyRetries, ApiError.class).getStatusCode())
                .isEqualTo(HttpStatus.BAD_REQUEST);
        assertThat(rest.postForEntity("/api/v1/jobs", badTimeout, ApiError.class).getStatusCode())
                .isEqualTo(HttpStatus.BAD_REQUEST);
        assertThat(rest.postForEntity("/api/v1/jobs", hugeTimeout, ApiError.class).getStatusCode())
                .isEqualTo(HttpStatus.BAD_REQUEST);
    }

    @Test
    @DisplayName("a payload the handler could never execute is rejected at submission, not at execution")
    void rejectsAPayloadTheHandlerCannotUse() throws Exception {
        CreateJobRequest request = new CreateJobRequest(
                "bad-payload-" + UUID.randomUUID(), JobType.DELAY,
                objectMapper.readTree("{\"durationMs\": 999999999}"), 3, 60);

        ResponseEntity<ApiError> response = rest.postForEntity("/api/v1/jobs", request, ApiError.class);

        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.BAD_REQUEST);
        assertThat(response.getBody().error()).isEqualTo("PAYLOAD_VALIDATION_ERROR");
        assertThat(response.getBody().message()).contains("durationMs");
    }

    @Test
    void rejectsAnOversizedPayload() throws Exception {
        String big = "{\"durationMs\": 50, \"filler\": \"" + "x".repeat(9000) + "\"}";
        CreateJobRequest request = new CreateJobRequest(
                "big-" + UUID.randomUUID(), JobType.DELAY, objectMapper.readTree(big), 3, 60);

        ResponseEntity<ApiError> response = rest.postForEntity("/api/v1/jobs", request, ApiError.class);

        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.BAD_REQUEST);
        assertThat(response.getBody().message()).contains("exceeds");
    }

    @Test
    void handlesMalformedJsonSafely() {
        HttpHeaders headers = new HttpHeaders();
        headers.setContentType(MediaType.APPLICATION_JSON);
        HttpEntity<String> entity = new HttpEntity<>("{\"idempotencyKey\": ", headers);

        ResponseEntity<ApiError> response =
                rest.exchange("/api/v1/jobs", HttpMethod.POST, entity, ApiError.class);

        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.BAD_REQUEST);
        assertThat(response.getBody().error()).isEqualTo("MALFORMED_REQUEST");
        // No parser internals, no stack trace.
        assertThat(response.getBody().message()).doesNotContain("com.fasterxml");
    }

    @Test
    void rejectsAnUnknownJobType() {
        HttpHeaders headers = new HttpHeaders();
        headers.setContentType(MediaType.APPLICATION_JSON);
        HttpEntity<String> entity = new HttpEntity<>(
                "{\"idempotencyKey\":\"x\",\"jobType\":\"MINE_CRYPTO\",\"payload\":{}}", headers);

        ResponseEntity<ApiError> response =
                rest.exchange("/api/v1/jobs", HttpMethod.POST, entity, ApiError.class);

        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.BAD_REQUEST);
    }

    // ------------------------------------------------------------------
    // Cancellation
    // ------------------------------------------------------------------

    @Test
    @DisplayName("a job that has not been dispatched is cancelled without ever executing")
    void cancelsAQueuedJob() {
        // Persisted directly, with no outbox event, so no worker will pick it up.
        JobEntity queued = jobRepository.saveAndFlush(JobFixtures.queuedDelay());

        ResponseEntity<JobResponse> response = rest.postForEntity(
                "/api/v1/jobs/{id}/cancel", null, JobResponse.class, queued.getId());

        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.OK);
        assertThat(response.getBody().status()).isEqualTo(JobStatus.CANCELLED);
        assertThat(reload(queued.getId()).getStartedAt()).isNull();
    }

    @Test
    void refusesToCancelACompletedJob() {
        JobEntity completed = jobRepository.saveAndFlush(JobEntity.builder()
                .idempotencyKey("completed-" + UUID.randomUUID())
                .jobType(JobType.DELAY)
                .payload("{\"durationMs\":10}")
                .status(JobStatus.COMPLETED)
                .maxRetries(3)
                .timeoutSeconds(60)
                .build());

        ResponseEntity<ApiError> response = rest.postForEntity(
                "/api/v1/jobs/{id}/cancel", null, ApiError.class, completed.getId());

        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.CONFLICT);
        assertThat(response.getBody().error()).isEqualTo("JOB_STATE_CONFLICT");
    }

    @Test
    void cancellingAnUnknownJobReturns404() {
        ResponseEntity<ApiError> response = rest.postForEntity(
                "/api/v1/jobs/{id}/cancel", null, ApiError.class, UUID.randomUUID());

        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.NOT_FOUND);
    }

    // ------------------------------------------------------------------
    // Health
    // ------------------------------------------------------------------

    @Test
    @DisplayName("actuator reports PostgreSQL, Redis and Kafka, and nothing dangerous is exposed")
    void healthEndpointCoversEveryDependency() {
        ResponseEntity<String> health = rest.getForEntity("/actuator/health", String.class);

        assertThat(health.getStatusCode()).isEqualTo(HttpStatus.OK);
        assertThat(health.getBody()).contains("\"status\":\"UP\"");
        assertThat(health.getBody()).contains("db", "redis", "kafka");

        assertThat(rest.getForEntity("/actuator/env", String.class).getStatusCode())
                .isEqualTo(HttpStatus.NOT_FOUND);
        assertThat(rest.getForEntity("/actuator/beans", String.class).getStatusCode())
                .isEqualTo(HttpStatus.NOT_FOUND);
    }
}
