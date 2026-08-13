package com.taskforge.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.taskforge.cache.JobCache;
import com.taskforge.dto.CreateJobRequest;
import com.taskforge.dto.JobResponse;
import com.taskforge.entity.JobEntity;
import com.taskforge.exception.JobConflictException;
import com.taskforge.exception.JobNotFoundException;
import com.taskforge.model.JobStatus;
import com.taskforge.model.JobType;
import com.taskforge.repository.JobRepository;
import com.taskforge.support.JobFixtures;
import com.taskforge.validation.JobPayloadValidator;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.dao.DataIntegrityViolationException;

import java.util.Optional;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class JobServiceTest {

    private static final String PAYLOAD = "{\"durationMs\":100}";

    @Mock
    private JobRepository jobRepository;
    @Mock
    private JobCreator jobCreator;
    @Mock
    private JobCache jobCache;
    @Mock
    private JobPayloadValidator payloadValidator;

    private final JobMapper jobMapper = new JobMapper(new ObjectMapper());
    private JobService jobService;

    @BeforeEach
    void setUp() {
        jobService = new JobService(jobRepository, jobCreator, jobMapper, jobCache, payloadValidator);
    }

    private CreateJobRequest request(String idempotencyKey) {
        try {
            return new CreateJobRequest(
                    idempotencyKey, JobType.DELAY, new ObjectMapper().readTree(PAYLOAD), 3, 60);
        } catch (Exception e) {
            throw new IllegalStateException(e);
        }
    }

    // ------------------------------------------------------------------
    // Submission and idempotency
    // ------------------------------------------------------------------

    @Test
    void createsANewJob() {
        JobEntity created = JobFixtures.queuedDelay();
        when(payloadValidator.validateAndSerialize(any(), any())).thenReturn(PAYLOAD);
        when(jobRepository.findByIdempotencyKey("fresh-key")).thenReturn(Optional.empty());
        when(jobCreator.createWithOutboxEvent(any(), eq(PAYLOAD))).thenReturn(created);

        JobSubmissionResult result = jobService.submit(request("fresh-key"));

        assertThat(result.created()).isTrue();
        assertThat(result.job().id()).isEqualTo(created.getId());
        assertThat(result.job().status()).isEqualTo(JobStatus.QUEUED);
    }

    @Test
    @DisplayName("a repeated idempotency key returns the original job without creating another")
    void returnsExistingJobForKnownKey() {
        JobEntity existing = JobFixtures.queuedDelay();
        when(payloadValidator.validateAndSerialize(any(), any())).thenReturn(PAYLOAD);
        when(jobRepository.findByIdempotencyKey("used-key")).thenReturn(Optional.of(existing));

        JobSubmissionResult result = jobService.submit(request("used-key"));

        assertThat(result.created()).isFalse();
        assertThat(result.job().id()).isEqualTo(existing.getId());
        verify(jobCreator, never()).createWithOutboxEvent(any(), anyString());
    }

    @Test
    @DisplayName("losing the unique-index race collapses onto the winner's job rather than failing")
    void resolvesConcurrentSubmissionRace() {
        JobEntity winner = JobFixtures.queuedDelay();
        when(payloadValidator.validateAndSerialize(any(), any())).thenReturn(PAYLOAD);
        // First lookup finds nothing (both requests raced past it); the retry finds the winner.
        when(jobRepository.findByIdempotencyKey("raced-key"))
                .thenReturn(Optional.empty())
                .thenReturn(Optional.of(winner));
        when(jobCreator.createWithOutboxEvent(any(), anyString()))
                .thenThrow(new DataIntegrityViolationException("duplicate key value violates ux_jobs_idempotency_key"));

        JobSubmissionResult result = jobService.submit(request("raced-key"));

        assertThat(result.created()).isFalse();
        assertThat(result.job().id()).isEqualTo(winner.getId());
    }

    @Test
    @DisplayName("a constraint violation that is not an idempotency race is not swallowed")
    void rethrowsWhenNoWinningRowExists() {
        when(payloadValidator.validateAndSerialize(any(), any())).thenReturn(PAYLOAD);
        when(jobRepository.findByIdempotencyKey("odd-key")).thenReturn(Optional.empty());
        when(jobCreator.createWithOutboxEvent(any(), anyString()))
                .thenThrow(new DataIntegrityViolationException("some other constraint"));

        assertThatThrownBy(() -> jobService.submit(request("odd-key")))
                .isInstanceOf(DataIntegrityViolationException.class);
    }

    // ------------------------------------------------------------------
    // Reads
    // ------------------------------------------------------------------

    @Test
    void servesCachedJobsWithoutTouchingTheDatabase() {
        JobEntity job = JobFixtures.queuedDelay();
        JobResponse cached = jobMapper.toResponse(job);
        when(jobCache.get(job.getId())).thenReturn(Optional.of(cached));

        JobResponse result = jobService.getJob(job.getId());

        assertThat(result.id()).isEqualTo(job.getId());
        verify(jobRepository, never()).findById(any());
    }

    @Test
    void fallsBackToTheDatabaseAndRepopulatesTheCache() {
        JobEntity job = JobFixtures.queuedDelay();
        when(jobCache.get(job.getId())).thenReturn(Optional.empty());
        when(jobRepository.findById(job.getId())).thenReturn(Optional.of(job));

        JobResponse result = jobService.getJob(job.getId());

        assertThat(result.id()).isEqualTo(job.getId());
        verify(jobCache).put(any(JobResponse.class));
    }

    @Test
    void unknownJobIsNotFound() {
        UUID unknown = UUID.randomUUID();
        when(jobCache.get(unknown)).thenReturn(Optional.empty());
        when(jobRepository.findById(unknown)).thenReturn(Optional.empty());

        assertThatThrownBy(() -> jobService.getJob(unknown)).isInstanceOf(JobNotFoundException.class);
    }

    // ------------------------------------------------------------------
    // Cancellation
    // ------------------------------------------------------------------

    @Test
    @DisplayName("a job that has not started is cancelled outright")
    void cancelsAJobThatHasNotStarted() {
        JobEntity job = JobFixtures.queuedDelay();
        when(jobRepository.cancelIfNotStarted(eq(job.getId()), any())).thenReturn(1);
        when(jobRepository.findById(job.getId())).thenReturn(Optional.of(job));

        jobService.cancel(job.getId());

        verify(jobRepository, never()).requestCancelIfRunning(any(), any());
        verify(jobCache).invalidate(job.getId());
    }

    @Test
    @DisplayName("a running job is asked to stop rather than cancelled directly")
    void requestsCancellationForARunningJob() {
        JobEntity job = JobFixtures.queuedDelay();
        when(jobRepository.cancelIfNotStarted(eq(job.getId()), any())).thenReturn(0);
        when(jobRepository.requestCancelIfRunning(eq(job.getId()), any())).thenReturn(1);
        when(jobRepository.findById(job.getId())).thenReturn(Optional.of(job));

        jobService.cancel(job.getId());

        verify(jobCache).invalidate(job.getId());
    }

    @Test
    void refusesToCancelATerminalJob() {
        JobEntity completed = JobEntity.builder()
                .idempotencyKey("done")
                .jobType(JobType.DELAY)
                .payload(PAYLOAD)
                .status(JobStatus.COMPLETED)
                .maxRetries(3)
                .timeoutSeconds(60)
                .build();

        when(jobRepository.cancelIfNotStarted(eq(completed.getId()), any())).thenReturn(0);
        when(jobRepository.requestCancelIfRunning(eq(completed.getId()), any())).thenReturn(0);
        when(jobRepository.findById(completed.getId())).thenReturn(Optional.of(completed));

        assertThatThrownBy(() -> jobService.cancel(completed.getId()))
                .isInstanceOf(JobConflictException.class)
                .hasMessageContaining("COMPLETED");
    }

    @Test
    @DisplayName("cancelling twice is a no-op, not an error")
    void repeatedCancellationIsIdempotent() {
        JobEntity alreadyRequested = JobEntity.builder()
                .idempotencyKey("stopping")
                .jobType(JobType.DELAY)
                .payload(PAYLOAD)
                .status(JobStatus.CANCEL_REQUESTED)
                .maxRetries(3)
                .timeoutSeconds(60)
                .build();

        when(jobRepository.cancelIfNotStarted(eq(alreadyRequested.getId()), any())).thenReturn(0);
        when(jobRepository.requestCancelIfRunning(eq(alreadyRequested.getId()), any())).thenReturn(0);
        when(jobRepository.findById(alreadyRequested.getId())).thenReturn(Optional.of(alreadyRequested));

        JobResponse response = jobService.cancel(alreadyRequested.getId());

        assertThat(response.status()).isEqualTo(JobStatus.CANCEL_REQUESTED);
    }

    @Test
    void cancellingAnUnknownJobIsNotFound() {
        UUID unknown = UUID.randomUUID();
        when(jobRepository.cancelIfNotStarted(eq(unknown), any())).thenReturn(0);
        when(jobRepository.requestCancelIfRunning(eq(unknown), any())).thenReturn(0);
        when(jobRepository.findById(unknown)).thenReturn(Optional.empty());

        assertThatThrownBy(() -> jobService.cancel(unknown)).isInstanceOf(JobNotFoundException.class);
    }
}
