package com.taskforge.it;

import com.taskforge.config.TaskForgeProperties;
import com.taskforge.dto.JobResponse;
import com.taskforge.entity.JobEntity;
import com.taskforge.entity.OutboxEventEntity;
import com.taskforge.kafka.JobEventType;
import com.taskforge.model.JobStatus;
import com.taskforge.model.JobType;
import com.taskforge.support.AbstractIntegrationTest;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;

import java.time.Duration;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/** Retries, exponential backoff and the dead-letter path. */
class RetryAndDeadLetterIT extends AbstractIntegrationTest {

    @Autowired
    private TaskForgeProperties properties;

    @Test
    @DisplayName("a job that fails twice then succeeds is retried and finishes COMPLETED")
    void retriesUntilTheJobSucceeds() {
        JobResponse submitted = submitSuccessfully(
                newRequest(JobType.FAIL_TEST, "{\"failUntilAttempt\": 2}", 3, 60));

        awaitStatus(submitted.id(), JobStatus.COMPLETED, Duration.ofSeconds(60));

        JobEntity completed = reload(submitted.id());
        assertThat(completed.getRetryCount())
                .as("two retries were consumed to get past two failing attempts")
                .isEqualTo(2);
        assertThat(completed.currentAttempt()).isEqualTo(3);
        assertThat(json(completed.getResult()).get("succeededOnAttempt").asInt()).isEqualTo(3);
        assertThat(completed.getNextAttemptAt()).isNull();
        assertThat(completed.getErrorMessage()).isNull();
    }

    @Test
    @DisplayName("the retry count is persisted, so it survives any process restarting")
    void retryProgressIsDurable() {
        JobResponse submitted = submitSuccessfully(
                newRequest(JobType.FAIL_TEST, "{\"failUntilAttempt\": 1}", 3, 60));

        awaitStatus(submitted.id(), JobStatus.COMPLETED, Duration.ofSeconds(60));

        // Read straight from PostgreSQL: nothing about the retry lives only in memory.
        JobEntity fromDatabase = jobRepository.findById(submitted.id()).orElseThrow();
        assertThat(fromDatabase.getRetryCount()).isEqualTo(1);
    }

    @Test
    @DisplayName("exhausting the retry budget dead-letters the job and emits a DLQ event")
    void deadLettersAJobThatNeverSucceeds() {
        JobResponse submitted = submitSuccessfully(
                newRequest(JobType.FAIL_TEST, "{\"failUntilAttempt\": 99}", 2, 60));

        awaitStatus(submitted.id(), JobStatus.DEAD_LETTERED, Duration.ofSeconds(60));

        JobEntity deadLettered = reload(submitted.id());
        assertThat(deadLettered.getRetryCount())
                .as("the full retry budget was used")
                .isEqualTo(2);
        assertThat(deadLettered.getErrorMessage()).contains("retry budget exhausted");
        assertThat(deadLettered.getCompletedAt()).isNotNull();
        assertThat(deadLettered.getLeaseExpiresAt()).isNull();

        // A DLQ event was written transactionally with the status change, and published.
        List<OutboxEventEntity> events =
                outboxEventRepository.findByAggregateIdOrderByCreatedAt(submitted.id());
        List<OutboxEventEntity> dlqEvents = events.stream()
                .filter(event -> event.getEventType().equals(JobEventType.JOB_DEAD_LETTERED.name()))
                .toList();

        assertThat(dlqEvents).hasSize(1);
        assertThat(dlqEvents.get(0).getTopic()).isEqualTo(properties.getTopics().getDlq());
        assertThat(dlqEvents.get(0).getPayload()).contains("JOB_DEAD_LETTERED");

        org.awaitility.Awaitility.await("DLQ event to be published")
                .atMost(Duration.ofSeconds(20))
                .untilAsserted(() -> assertThat(
                        outboxEventRepository.findById(dlqEvents.get(0).getId()).orElseThrow().getStatus())
                        .isEqualTo(OutboxEventEntity.Status.PUBLISHED));
    }

    @Test
    @DisplayName("a job may be dead-lettered inspected through the normal GET endpoint")
    void deadLetteredJobsRemainReadableThroughTheApi() {
        JobResponse submitted = submitSuccessfully(
                newRequest(JobType.FAIL_TEST, "{\"failUntilAttempt\": 99}", 0, 60));

        awaitStatus(submitted.id(), JobStatus.DEAD_LETTERED, Duration.ofSeconds(60));

        JobResponse fetched = rest.getForObject("/api/v1/jobs/{id}", JobResponse.class, submitted.id());

        assertThat(fetched.status()).isEqualTo(JobStatus.DEAD_LETTERED);
        assertThat(fetched.errorMessage()).isNotBlank();
        assertThat(fetched.retryCount()).isZero();
    }

    @Test
    @DisplayName("a permanent failure goes straight to FAILED without spending a single retry")
    void permanentFailuresSkipRetriesEntirely() {
        JobResponse submitted = submitSuccessfully(
                newRequest(JobType.FAIL_TEST, "{\"failUntilAttempt\": 99, \"mode\": \"PERMANENT\"}", 5, 60));

        awaitStatus(submitted.id(), JobStatus.FAILED, Duration.ofSeconds(60));

        JobEntity failed = reload(submitted.id());
        assertThat(failed.getRetryCount())
                .as("no retry should have been consumed")
                .isZero();
        assertThat(failed.getErrorMessage()).contains("FAIL_TEST attempt 1");
        assertThat(failed.getNextAttemptAt()).isNull();
    }

    @Test
    @DisplayName("backoff is honoured: consecutive attempts are separated by a growing delay")
    void appliesExponentialBackoffBetweenAttempts() {
        JobResponse submitted = submitSuccessfully(
                newRequest(JobType.FAIL_TEST, "{\"failUntilAttempt\": 2}", 3, 60));

        awaitStatus(submitted.id(), JobStatus.COMPLETED, Duration.ofSeconds(60));

        JobEntity completed = reload(submitted.id());
        Duration totalElapsed = Duration.between(completed.getCreatedAt(), completed.getCompletedAt());

        // Test config: 200ms initial delay, x2. Two retries wait at least 200ms + 400ms.
        Duration minimumBackoff = properties.getRetry().getInitialDelay()
                .plus(properties.getRetry().getInitialDelay().multipliedBy(2));

        assertThat(totalElapsed)
                .as("the job cannot have finished faster than its mandatory backoff")
                .isGreaterThanOrEqualTo(minimumBackoff);
    }
}
