package com.taskforge.recovery;

import com.taskforge.cache.JobCache;
import com.taskforge.cache.WorkerHeartbeatStore;
import com.taskforge.config.TaskForgeProperties;
import com.taskforge.entity.JobEntity;
import com.taskforge.repository.JobRepository;
import com.taskforge.service.JobLifecycleService;
import com.taskforge.support.JobFixtures;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.junit.jupiter.MockitoSettings;
import org.mockito.quality.Strictness;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Recovery has to distinguish three situations that look similar from the database alone:
 * a dead worker, a slow worker, and a job that is simply taking too long.
 */
@ExtendWith(MockitoExtension.class)
@MockitoSettings(strictness = Strictness.LENIENT)
class JobRecoveryServiceTest {

    @Mock
    private JobRepository jobRepository;
    @Mock
    private JobLifecycleService jobLifecycleService;
    @Mock
    private WorkerHeartbeatStore heartbeatStore;
    @Mock
    private JobCache jobCache;

    private TaskForgeProperties properties;
    private JobRecoveryService recoveryService;

    @BeforeEach
    void setUp() {
        properties = new TaskForgeProperties();
        properties.getRecovery().setLiveWorkerGrace(Duration.ofSeconds(30));
        properties.getRecovery().setBatchSize(50);
        recoveryService = new JobRecoveryService(
                jobRepository, jobLifecycleService, heartbeatStore, jobCache, properties);
    }

    private void givenCandidate(JobEntity job) {
        when(jobRepository.lockAbandonedJobs(any(), anyInt())).thenReturn(List.of(job.getId()));
        when(jobRepository.findById(job.getId())).thenReturn(Optional.of(job));
    }

    @Test
    @DisplayName("an expired lease from a worker that stopped heartbeating is reclaimed at once")
    void reclaimsJobsFromDeadWorkers() {
        Instant now = Instant.now();
        JobEntity job = JobFixtures.running("dead-worker", now.minusSeconds(120), now.minusSeconds(30), 3600);
        givenCandidate(job);
        when(heartbeatStore.isAlive("dead-worker")).thenReturn(false);
        when(jobRepository.markTimedOutByRecovery(eq(job.getId()), anyString(), any())).thenReturn(1);

        int recovered = recoveryService.recoverAbandonedJobs();

        assertThat(recovered).isEqualTo(1);
        verify(jobLifecycleService).resolveTimedOut(eq(job), anyString(), any());
        verify(jobCache).invalidate(job.getId());
    }

    @Test
    @DisplayName("a live worker gets a grace period, because a late renewal is not a death")
    void grantsGraceToJobsWhoseWorkerIsStillAlive() {
        Instant now = Instant.now();
        // Lease lapsed 5 seconds ago; the 30s grace has not run out.
        JobEntity job = JobFixtures.running("slow-worker", now.minusSeconds(60), now.minusSeconds(5), 3600);
        givenCandidate(job);
        when(heartbeatStore.isAlive("slow-worker")).thenReturn(true);

        int recovered = recoveryService.recoverAbandonedJobs();

        assertThat(recovered).isZero();
        verify(jobRepository, never()).markTimedOutByRecovery(any(), anyString(), any());
        verify(jobLifecycleService, never()).resolveTimedOut(any(), anyString(), any());
    }

    @Test
    @DisplayName("a worker that heartbeats but never renews is eventually reclaimed anyway")
    void reclaimsAfterGraceEvenFromALiveWorker() {
        Instant now = Instant.now();
        JobEntity job = JobFixtures.running("stuck-worker", now.minusSeconds(300), now.minusSeconds(120), 3600);
        givenCandidate(job);
        when(heartbeatStore.isAlive("stuck-worker")).thenReturn(true);
        when(jobRepository.markTimedOutByRecovery(eq(job.getId()), anyString(), any())).thenReturn(1);

        int recovered = recoveryService.recoverAbandonedJobs();

        assertThat(recovered).isEqualTo(1);
        verify(jobLifecycleService).resolveTimedOut(eq(job), anyString(), any());
    }

    @Test
    @DisplayName("a job past its own timeout is reclaimed regardless of lease or heartbeat")
    void reclaimsJobsThatOutlivedTheirTimeout() {
        Instant now = Instant.now();
        // Lease is still valid, but the job has been running for 60s with a 10s timeout.
        JobEntity job = JobFixtures.running("busy-worker", now.minusSeconds(60), now.plusSeconds(30), 10);
        givenCandidate(job);
        when(jobRepository.markTimedOutByRecovery(eq(job.getId()), anyString(), any())).thenReturn(1);

        int recovered = recoveryService.recoverAbandonedJobs();

        assertThat(recovered).isEqualTo(1);
        verify(heartbeatStore, never()).isAlive(anyString());
    }

    @Test
    @DisplayName("if the owning worker finishes first, recovery quietly does nothing")
    void doesNothingWhenTheJobIsFinishedConcurrently() {
        Instant now = Instant.now();
        JobEntity job = JobFixtures.running("racing-worker", now.minusSeconds(120), now.minusSeconds(30), 3600);
        givenCandidate(job);
        when(heartbeatStore.isAlive("racing-worker")).thenReturn(false);
        // The conditional UPDATE matched nothing: the worker completed the job in between.
        when(jobRepository.markTimedOutByRecovery(eq(job.getId()), anyString(), any())).thenReturn(0);

        int recovered = recoveryService.recoverAbandonedJobs();

        assertThat(recovered).isZero();
        verify(jobLifecycleService, never()).resolveTimedOut(any(), anyString(), any());
    }

    @Test
    void doesNothingWhenThereAreNoCandidates() {
        when(jobRepository.lockAbandonedJobs(any(), anyInt())).thenReturn(List.of());

        assertThat(recoveryService.recoverAbandonedJobs()).isZero();
        verify(jobRepository, never()).findById(any());
    }
}
