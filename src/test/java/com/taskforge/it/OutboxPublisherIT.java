package com.taskforge.it;

import com.taskforge.config.TaskForgeProperties;
import com.taskforge.entity.JobEntity;
import com.taskforge.entity.OutboxEventEntity;
import com.taskforge.kafka.JobEvent;
import com.taskforge.model.JobStatus;
import com.taskforge.model.JobType;
import com.taskforge.support.AbstractIntegrationTest;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.JdbcTemplate;

import java.time.Duration;
import java.time.Instant;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * The transactional outbox under a publication failure.
 *
 * <p>The failure is real rather than mocked: the event is written with a topic name Kafka
 * cannot accept, so the actual producer actually rejects it, on the actual background
 * publisher. Repairing the topic afterwards stands in for the broker coming back.
 *
 * <p>Testing it this way — through the running scheduler rather than by driving the
 * publisher by hand — also avoids depending on no other publisher touching the shared
 * database, which every other test class in this suite is doing concurrently.
 */
class OutboxPublisherIT extends AbstractIntegrationTest {

    private static final String UNPUBLISHABLE_TOPIC = "taskforge.invalid topic!";

    @Autowired
    private JdbcTemplate jdbcTemplate;

    @Autowired
    private TaskForgeProperties properties;

    @Test
    @DisplayName("a job whose event cannot be published is delayed, never lost, and runs once publication recovers")
    void publicationFailureDelaysButNeverStrandsAJob() {
        // A job that exists in PostgreSQL but has not yet been announced to anyone.
        JobEntity job = jobRepository.saveAndFlush(JobEntity.builder()
                .idempotencyKey("outbox-" + UUID.randomUUID())
                .jobType(JobType.DELAY)
                .payload("{\"durationMs\":100}")
                .status(JobStatus.QUEUED)
                .maxRetries(3)
                .timeoutSeconds(60)
                .build());

        OutboxEventEntity event = outboxEventRepository.saveAndFlush(new OutboxEventEntity(
                job.getId(),
                "JOB_SUBMITTED",
                UNPUBLISHABLE_TOPIC,
                toJson(JobEvent.submitted(job.getId(), JobType.DELAY, 1)),
                Instant.now()));

        // --- While publication keeps failing ---
        Awaitility.await("the publisher to keep retrying the failing event")
                .atMost(Duration.ofSeconds(30))
                .pollInterval(Duration.ofMillis(200))
                .untilAsserted(() -> {
                    OutboxEventEntity current = outboxEventRepository.findById(event.getId()).orElseThrow();
                    assertThat(current.getAttemptCount())
                            .as("the publisher should be retrying, not giving up")
                            .isGreaterThanOrEqualTo(2);
                    assertThat(current.getStatus()).isEqualTo(OutboxEventEntity.Status.PENDING);
                    assertThat(current.getLastError()).isNotBlank();
                    assertThat(current.getPublishedAt()).isNull();
                });

        // The job itself is untouched: not executed, not failed, and certainly not lost.
        JobEntity duringOutage = reload(job.getId());
        assertThat(duringOutage.getStatus()).isEqualTo(JobStatus.QUEUED);
        assertThat(duringOutage.getStartedAt()).isNull();

        // --- Publication becomes possible again ---
        jdbcTemplate.update("UPDATE outbox_events SET topic = ? WHERE id = ?",
                properties.getTopics().getJobs(), event.getId());

        Awaitility.await("the event to finally publish")
                .atMost(Duration.ofSeconds(30))
                .pollInterval(Duration.ofMillis(200))
                .untilAsserted(() -> {
                    OutboxEventEntity current = outboxEventRepository.findById(event.getId()).orElseThrow();
                    assertThat(current.getStatus()).isEqualTo(OutboxEventEntity.Status.PUBLISHED);
                    assertThat(current.getPublishedAt()).isNotNull();
                    assertThat(current.getLastError()).isNull();
                });

        // And the job that was stuck behind it now runs to completion.
        awaitStatus(job.getId(), JobStatus.COMPLETED, Duration.ofSeconds(60));
        assertThat(reload(job.getId()).getWorkerId()).isNotBlank();
    }

    @Test
    @DisplayName("an event that has been published is never published a second time")
    void publishedEventsAreNotRepublished() throws Exception {
        JobEntity job = jobRepository.saveAndFlush(JobEntity.builder()
                .idempotencyKey("outbox-once-" + UUID.randomUUID())
                .jobType(JobType.DELAY)
                .payload("{\"durationMs\":50}")
                .status(JobStatus.QUEUED)
                .maxRetries(3)
                .timeoutSeconds(60)
                .build());

        OutboxEventEntity event = outboxEventRepository.saveAndFlush(new OutboxEventEntity(
                job.getId(),
                "JOB_SUBMITTED",
                properties.getTopics().getJobs(),
                toJson(JobEvent.submitted(job.getId(), JobType.DELAY, 1)),
                Instant.now()));

        Awaitility.await("the event to publish")
                .atMost(Duration.ofSeconds(30))
                .pollInterval(Duration.ofMillis(200))
                .untilAsserted(() -> assertThat(
                        outboxEventRepository.findById(event.getId()).orElseThrow().getStatus())
                        .isEqualTo(OutboxEventEntity.Status.PUBLISHED));

        OutboxEventEntity afterFirstPublish = outboxEventRepository.findById(event.getId()).orElseThrow();

        // Give the publisher several more polling passes to prove it leaves the row alone.
        Thread.sleep(2000);

        OutboxEventEntity later = outboxEventRepository.findById(event.getId()).orElseThrow();
        assertThat(later.getAttemptCount()).isEqualTo(afterFirstPublish.getAttemptCount());
        assertThat(later.getPublishedAt()).isEqualTo(afterFirstPublish.getPublishedAt());
    }

    private String toJson(JobEvent event) {
        try {
            return objectMapper.writeValueAsString(event);
        } catch (Exception e) {
            throw new IllegalStateException(e);
        }
    }
}
