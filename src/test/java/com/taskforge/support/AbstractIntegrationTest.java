package com.taskforge.support;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.taskforge.dto.CreateJobRequest;
import com.taskforge.dto.JobResponse;
import com.taskforge.entity.JobEntity;
import com.taskforge.model.JobStatus;
import com.taskforge.model.JobType;
import com.taskforge.repository.JobRepository;
import com.taskforge.repository.OutboxEventRepository;
import org.awaitility.Awaitility;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.client.TestRestTemplate;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.kafka.KafkaContainer;
import org.testcontainers.utility.DockerImageName;

import java.time.Duration;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Base for integration tests: real PostgreSQL, real Redis, real Kafka.
 *
 * <p>Nothing here is mocked, because the behaviour under test <em>is</em> the interaction
 * with these systems — {@code SKIP LOCKED}, unique-constraint races, at-least-once delivery
 * and TTL expiry have no meaningful in-memory equivalent.
 *
 * <p>The containers are started once for the whole JVM and shared by every test class. The
 * profiles are {@code test} (fast timers) plus {@code all} (this process plays both the API
 * and the worker role), so a single Spring context covers the full submit-to-execute path.
 */
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@ActiveProfiles({"test", "all"})
public abstract class AbstractIntegrationTest {

    static final PostgreSQLContainer<?> POSTGRES =
            new PostgreSQLContainer<>(DockerImageName.parse("postgres:16-alpine"))
                    .withDatabaseName("taskforge")
                    .withUsername("taskforge")
                    .withPassword("taskforge");

    static final GenericContainer<?> REDIS =
            new GenericContainer<>(DockerImageName.parse("redis:7-alpine"))
                    .withExposedPorts(6379);

    static final KafkaContainer KAFKA =
            new KafkaContainer(DockerImageName.parse("apache/kafka:3.8.1"));

    static {
        POSTGRES.start();
        REDIS.start();
        KAFKA.start();
    }

    @DynamicPropertySource
    static void registerContainerProperties(DynamicPropertyRegistry registry) {
        registry.add("spring.datasource.url", POSTGRES::getJdbcUrl);
        registry.add("spring.datasource.username", POSTGRES::getUsername);
        registry.add("spring.datasource.password", POSTGRES::getPassword);

        registry.add("spring.data.redis.host", REDIS::getHost);
        registry.add("spring.data.redis.port", () -> REDIS.getMappedPort(6379));

        registry.add("spring.kafka.bootstrap-servers", KAFKA::getBootstrapServers);
    }

    @Autowired
    protected JobRepository jobRepository;

    @Autowired
    protected OutboxEventRepository outboxEventRepository;

    @Autowired
    protected TestRestTemplate rest;

    @Autowired
    protected ObjectMapper objectMapper;

    // ------------------------------------------------------------------
    // Helpers
    // ------------------------------------------------------------------

    protected CreateJobRequest newRequest(JobType type, String payloadJson, int maxRetries, int timeoutSeconds) {
        try {
            return new CreateJobRequest(
                    "it-" + UUID.randomUUID(), type, objectMapper.readTree(payloadJson), maxRetries, timeoutSeconds);
        } catch (Exception e) {
            throw new IllegalStateException("invalid test payload", e);
        }
    }

    protected ResponseEntity<JobResponse> submit(CreateJobRequest request) {
        return rest.postForEntity("/api/v1/jobs", request, JobResponse.class);
    }

    protected JobResponse submitSuccessfully(CreateJobRequest request) {
        ResponseEntity<JobResponse> response = submit(request);
        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.CREATED);
        return response.getBody();
    }

    /**
     * Parses a {@code jsonb} column. Asserting on parsed values rather than raw text matters:
     * PostgreSQL re-renders jsonb in its own canonical form (notably a space after each
     * colon), so string matching on stored JSON is brittle.
     */
    protected JsonNode json(String raw) {
        try {
            return objectMapper.readTree(raw);
        } catch (Exception e) {
            throw new AssertionError("expected valid JSON but got: " + raw, e);
        }
    }

    protected JobEntity reload(UUID jobId) {
        return jobRepository.findById(jobId).orElseThrow(() -> new AssertionError("job " + jobId + " vanished"));
    }

    /** Waits for a job to reach a status, failing with the status it actually reached. */
    protected void awaitStatus(UUID jobId, JobStatus expected, Duration timeout) {
        Awaitility.await("job %s to reach %s".formatted(jobId, expected))
                .atMost(timeout)
                .pollInterval(Duration.ofMillis(100))
                .untilAsserted(() -> assertThat(reload(jobId).getStatus()).isEqualTo(expected));
    }

    protected void awaitStatus(UUID jobId, JobStatus expected) {
        awaitStatus(jobId, expected, Duration.ofSeconds(30));
    }
}
