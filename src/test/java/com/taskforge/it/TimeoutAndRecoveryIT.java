package com.taskforge.it;

import com.taskforge.cache.WorkerHeartbeatStore;
import com.taskforge.dto.JobResponse;
import com.taskforge.entity.JobEntity;
import com.taskforge.model.JobStatus;
import com.taskforge.model.JobType;
import com.taskforge.model.WorkerHeartbeat;
import com.taskforge.recovery.JobRecoveryService;
import com.taskforge.support.AbstractIntegrationTest;
import com.taskforge.support.JobFixtures;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;

import java.time.Duration;
import java.time.Instant;

import static org.assertj.core.api.Assertions.assertThat;

/** Timeouts, lease expiry, stale-worker detection and abandoned-job recovery. */
class TimeoutAndRecoveryIT extends AbstractIntegrationTest {

    @Autowired
    private JobRecoveryService jobRecoveryService;

    @Autowired
    private WorkerHeartbeatStore heartbeatStore;

    @Test
    @DisplayName("a job that outlives its timeout is stopped and follows the dead-letter path")
    void enforcesJobTimeouts() {
        // A 60s sleep with a 2s timeout and no retries: the only possible ending is
        // TIMED_OUT followed immediately by DEAD_LETTERED.
        JobResponse submitted = submitSuccessfully(
                newRequest(JobType.DELAY, "{\"durationMs\": 60000}", 0, 2));

        awaitStatus(submitted.id(), JobStatus.DEAD_LETTERED, Duration.ofSeconds(60));

        JobEntity job = reload(submitted.id());
        assertThat(job.getErrorMessage()).containsIgnoringCase("timeout");
        assertThat(job.getLeaseExpiresAt()).isNull();
        assertThat(Duration.between(job.getStartedAt(), job.getCompletedAt()))
                .as("it must have been stopped near its deadline, not after the full 60s")
                .isLessThan(Duration.ofSeconds(30));
    }

    @Test
    @DisplayName("a timed-out job is retried when it still has budget")
    void retriesAfterATimeoutWhenBudgetRemains() {
        JobResponse submitted = submitSuccessfully(
                newRequest(JobType.DELAY, "{\"durationMs\": 60000}", 2, 2));

        // It will time out, retry, time out again, and finally exhaust its budget.
        awaitStatus(submitted.id(), JobStatus.DEAD_LETTERED, Duration.ofSeconds(90));

        JobEntity job = reload(submitted.id());
        assertThat(job.getRetryCount())
                .as("timeouts consume the retry budget like any other transient failure")
                .isEqualTo(2);
    }

    @Test
    @DisplayName("a RUNNING job whose worker vanished is reclaimed and dead-lettered")
    void recoversAJobAbandonedByADeadWorker() {
        Instant now = Instant.now();
        // A worker that never existed: no heartbeat key, and a lease that lapsed a minute ago.
        JobEntity abandoned = jobRepository.saveAndFlush(JobEntity.builder()
                .idempotencyKey("abandoned-" + java.util.UUID.randomUUID())
                .jobType(JobType.DELAY)
                .payload("{\"durationMs\":1000}")
                .status(JobStatus.RUNNING)
                .maxRetries(0)
                .timeoutSeconds(3600)
                .workerId("ghost-worker")
                .startedAt(now.minusSeconds(120))
                .leaseExpiresAt(now.minusSeconds(60))
                .build());

        assertThat(heartbeatStore.isAlive("ghost-worker")).isFalse();

        int recovered = jobRecoveryService.recoverAbandonedJobs();

        assertThat(recovered).isGreaterThanOrEqualTo(1);
        JobEntity result = reload(abandoned.getId());
        assertThat(result.getStatus()).isEqualTo(JobStatus.DEAD_LETTERED);
        assertThat(result.getErrorMessage()).contains("ghost-worker");
        assertThat(result.getErrorMessage()).containsIgnoringCase("lease expired");
    }

    @Test
    @DisplayName("an abandoned job with retry budget goes back into the queue and runs again")
    void recoveredJobsAreRetriedAndEventuallyExecute() {
        Instant now = Instant.now();
        JobEntity abandoned = jobRepository.saveAndFlush(JobEntity.builder()
                .idempotencyKey("recovered-" + java.util.UUID.randomUUID())
                .jobType(JobType.DELAY)
                .payload("{\"durationMs\":100}")
                .status(JobStatus.RUNNING)
                .maxRetries(3)
                .timeoutSeconds(3600)
                .workerId("ghost-worker-2")
                .startedAt(now.minusSeconds(120))
                .leaseExpiresAt(now.minusSeconds(60))
                .build());

        jobRecoveryService.recoverAbandonedJobs();

        // Recovery hands it to the retry scheduler, which re-queues and re-announces it;
        // a live worker then picks it up and finishes it.
        awaitStatus(abandoned.getId(), JobStatus.COMPLETED, Duration.ofSeconds(60));

        JobEntity completed = reload(abandoned.getId());
        assertThat(completed.getRetryCount()).isEqualTo(1);
        assertThat(completed.getWorkerId()).isNotEqualTo("ghost-worker-2");
    }

    @Test
    @DisplayName("a job whose lease is still valid is left strictly alone")
    void doesNotTouchJobsWithAValidLease() {
        Instant now = Instant.now();
        JobEntity healthy = jobRepository.saveAndFlush(JobFixtures.running(
                "healthy-worker", now.minusSeconds(5), now.plusSeconds(600), 3600));

        jobRecoveryService.recoverAbandonedJobs();

        assertThat(reload(healthy.getId()).getStatus()).isEqualTo(JobStatus.RUNNING);
        assertThat(reload(healthy.getId()).getWorkerId()).isEqualTo("healthy-worker");
    }

    @Test
    @DisplayName("a worker becomes stale when its heartbeat key expires")
    void heartbeatsExpireAndMarkWorkersStale() {
        String workerId = "ttl-worker-" + java.util.UUID.randomUUID();
        heartbeatStore.publish(
                new WorkerHeartbeat(workerId, Instant.now(), Instant.now(), "UP", 0, 4),
                Duration.ofSeconds(2));

        assertThat(heartbeatStore.isAlive(workerId)).isTrue();
        assertThat(heartbeatStore.find(workerId)).isPresent();

        Awaitility.await("heartbeat key to expire")
                .atMost(Duration.ofSeconds(15))
                .pollInterval(Duration.ofMillis(250))
                .untilAsserted(() -> assertThat(heartbeatStore.isAlive(workerId)).isFalse());
    }

    @Test
    @DisplayName("the live worker in this process publishes a heartbeat others can see")
    void liveWorkersAreDiscoverable() {
        Awaitility.await("this process to register a heartbeat")
                .atMost(Duration.ofSeconds(15))
                .pollInterval(Duration.ofMillis(250))
                .untilAsserted(() -> assertThat(heartbeatStore.listWorkers()).isNotEmpty());

        assertThat(heartbeatStore.listWorkers())
                .allSatisfy(worker -> {
                    assertThat(worker.workerId()).isNotBlank();
                    assertThat(worker.status()).isEqualTo("UP");
                });
    }
}
