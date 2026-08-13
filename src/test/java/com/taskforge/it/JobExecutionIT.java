package com.taskforge.it;

import com.taskforge.config.TaskForgeProperties;
import com.taskforge.dto.JobResponse;
import com.taskforge.entity.JobEntity;
import com.taskforge.kafka.JobEvent;
import com.taskforge.model.JobStatus;
import com.taskforge.model.JobType;
import com.taskforge.service.JobLifecycleService;
import com.taskforge.support.AbstractIntegrationTest;
import com.taskforge.support.JobFixtures;
import com.taskforge.worker.JobExecutionService;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

/** End-to-end execution: dispatch, claiming, concurrency and duplicate delivery. */
class JobExecutionIT extends AbstractIntegrationTest {

    @Autowired
    private JobExecutionService jobExecutionService;

    @Autowired
    private JobLifecycleService jobLifecycleService;

    @Autowired
    private KafkaTemplate<String, String> kafkaTemplate;

    @Autowired
    private TaskForgeProperties properties;

    @Test
    @DisplayName("submit -> QUEUED -> RUNNING -> COMPLETED, through Kafka and a real worker")
    void executesASubmittedJobEndToEnd() {
        JobResponse submitted = submitSuccessfully(newRequest(JobType.DELAY, "{\"durationMs\": 300}", 3, 60));
        assertThat(submitted.status()).isEqualTo(JobStatus.QUEUED);

        awaitStatus(submitted.id(), JobStatus.COMPLETED);

        JobEntity completed = reload(submitted.id());
        assertThat(completed.getWorkerId()).isNotBlank();
        assertThat(completed.getStartedAt()).isNotNull();
        assertThat(completed.getCompletedAt()).isNotNull();
        assertThat(completed.getLeaseExpiresAt()).isNull();
        assertThat(completed.getRetryCount()).isZero();
        assertThat(completed.getErrorMessage()).isNull();
        assertThat(json(completed.getResult()).get("sleptMs").asInt()).isEqualTo(300);

        // The outbox event that carried it is durably marked published.
        assertThat(outboxEventRepository.findByAggregateIdOrderByCreatedAt(submitted.id()))
                .singleElement()
                .satisfies(event -> {
                    assertThat(event.getStatus()).isEqualTo(com.taskforge.entity.OutboxEventEntity.Status.PUBLISHED);
                    assertThat(event.getPublishedAt()).isNotNull();
                });
    }

    @Test
    void executesAComputeJobAndStoresItsResult() {
        JobResponse submitted = submitSuccessfully(
                newRequest(JobType.COMPUTE, "{\"operation\":\"PRIME_COUNT\",\"n\":10000}", 3, 60));

        awaitStatus(submitted.id(), JobStatus.COMPLETED);

        assertThat(json(reload(submitted.id()).getResult()).get("primeCount").asInt()).isEqualTo(1229);
    }

    @Test
    @DisplayName("several jobs genuinely overlap in time rather than running one after another")
    void runsJobsConcurrently() {
        int jobCount = 4;
        List<UUID> jobIds = new ArrayList<>();
        for (int i = 0; i < jobCount; i++) {
            jobIds.add(submitSuccessfully(newRequest(JobType.DELAY, "{\"durationMs\": 1500}", 3, 60)).id());
        }

        for (UUID jobId : jobIds) {
            awaitStatus(jobId, JobStatus.COMPLETED, Duration.ofSeconds(60));
        }

        List<JobEntity> jobs = jobIds.stream().map(this::reload).toList();

        // Overlap is a timing-independent proof of concurrency: two jobs each started
        // before the other finished.
        boolean anyOverlap = false;
        for (int i = 0; i < jobs.size() && !anyOverlap; i++) {
            for (int j = i + 1; j < jobs.size(); j++) {
                JobEntity a = jobs.get(i);
                JobEntity b = jobs.get(j);
                if (a.getStartedAt().isBefore(b.getCompletedAt())
                        && b.getStartedAt().isBefore(a.getCompletedAt())) {
                    anyOverlap = true;
                    break;
                }
            }
        }

        assertThat(anyOverlap)
                .as("at least two of the %d jobs should have executed at the same time", jobCount)
                .isTrue();
    }

    @Test
    @DisplayName("when many workers race for one job, exactly one claim succeeds")
    void onlyOneWorkerCanClaimAJob() throws Exception {
        JobEntity job = jobRepository.saveAndFlush(JobFixtures.queuedDelay());
        int contenders = 8;

        ExecutorService pool = Executors.newFixedThreadPool(contenders);
        CountDownLatch startGun = new CountDownLatch(1);
        List<Future<Boolean>> attempts = new ArrayList<>();

        for (int i = 0; i < contenders; i++) {
            String workerId = "race-worker-" + i;
            attempts.add(pool.submit(() -> {
                startGun.await(5, TimeUnit.SECONDS);
                return jobLifecycleService.claim(job.getId(), workerId).isPresent();
            }));
        }
        startGun.countDown();

        int winners = 0;
        for (Future<Boolean> attempt : attempts) {
            if (attempt.get(30, TimeUnit.SECONDS)) {
                winners++;
            }
        }
        pool.shutdownNow();

        assertThat(winners)
                .as("the QUEUED -> RUNNING compare-and-swap must admit exactly one worker")
                .isEqualTo(1);

        JobEntity claimed = reload(job.getId());
        assertThat(claimed.getStatus()).isEqualTo(JobStatus.RUNNING);
        assertThat(claimed.getWorkerId()).startsWith("race-worker-");
        assertThat(claimed.getLeaseExpiresAt()).isNotNull();
    }

    @Test
    @DisplayName("a job already claimed by another worker cannot be claimed again")
    void aClaimedJobIsNotClaimableBySomeoneElse() {
        JobEntity job = jobRepository.saveAndFlush(JobFixtures.queuedDelay());

        assertThat(jobLifecycleService.claim(job.getId(), "worker-alpha")).isPresent();
        assertThat(jobLifecycleService.claim(job.getId(), "worker-beta")).isEmpty();

        assertThat(reload(job.getId()).getWorkerId()).isEqualTo("worker-alpha");
    }

    @Test
    @DisplayName("a duplicate Kafka delivery never re-executes a completed job")
    void duplicateDeliveryDoesNotReExecuteACompletedJob() throws Exception {
        JobResponse submitted = submitSuccessfully(newRequest(JobType.DELAY, "{\"durationMs\": 100}", 3, 60));
        awaitStatus(submitted.id(), JobStatus.COMPLETED);

        JobEntity afterFirstRun = reload(submitted.id());
        long versionAfterFirstRun = afterFirstRun.getVersion();

        // 1. The worker path invoked directly, exactly as a redelivered message would.
        jobExecutionService.executeJob(submitted.id());

        // 2. And a genuinely duplicated Kafka message for the same job.
        JobEvent duplicate = JobEvent.submitted(submitted.id(), JobType.DELAY, 1);
        kafkaTemplate.send(properties.getTopics().getJobs(), submitted.id().toString(),
                objectMapper.writeValueAsString(duplicate)).get(10, TimeUnit.SECONDS);
        Thread.sleep(3000);

        JobEntity afterDuplicates = reload(submitted.id());
        assertThat(afterDuplicates.getStatus()).isEqualTo(JobStatus.COMPLETED);
        assertThat(afterDuplicates.getVersion())
                .as("nothing should have written to the row")
                .isEqualTo(versionAfterFirstRun);
        assertThat(afterDuplicates.getCompletedAt()).isEqualTo(afterFirstRun.getCompletedAt());
        assertThat(afterDuplicates.getRetryCount()).isZero();
    }

    @Test
    @DisplayName("a terminal job is never claimable, whatever arrives from Kafka")
    void terminalJobsAreNeverClaimable() {
        JobEntity cancelled = jobRepository.saveAndFlush(JobEntity.builder()
                .idempotencyKey("terminal-" + UUID.randomUUID())
                .jobType(JobType.DELAY)
                .payload("{\"durationMs\":10}")
                .status(JobStatus.CANCELLED)
                .maxRetries(3)
                .timeoutSeconds(60)
                .build());

        assertThat(jobLifecycleService.claim(cancelled.getId(), "eager-worker")).isEmpty();
        assertThat(reload(cancelled.getId()).getStatus()).isEqualTo(JobStatus.CANCELLED);
    }

    @Test
    @DisplayName("a running job responds to cancellation and settles as CANCELLED")
    void cancelsARunningJobCooperatively() {
        JobResponse submitted = submitSuccessfully(newRequest(JobType.DELAY, "{\"durationMs\": 60000}", 0, 300));

        // Wait until a worker has actually picked it up.
        org.awaitility.Awaitility.await("job to start running")
                .atMost(Duration.ofSeconds(30))
                .pollInterval(Duration.ofMillis(100))
                .untilAsserted(() -> assertThat(reload(submitted.id()).getStatus())
                        .isIn(JobStatus.RUNNING, JobStatus.CANCEL_REQUESTED));

        rest.postForEntity("/api/v1/jobs/{id}/cancel", null, JobResponse.class, submitted.id());

        awaitStatus(submitted.id(), JobStatus.CANCELLED, Duration.ofSeconds(30));

        JobEntity cancelled = reload(submitted.id());
        assertThat(cancelled.getCompletedAt()).isNotNull();
        assertThat(cancelled.getLeaseExpiresAt()).isNull();
        // It stopped early rather than sleeping out its full 60 seconds.
        assertThat(Duration.between(cancelled.getStartedAt(), cancelled.getCompletedAt()))
                .isLessThan(Duration.ofSeconds(30));
    }
}
