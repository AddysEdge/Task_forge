package com.taskforge.service;

import com.taskforge.cache.JobCache;
import com.taskforge.dto.CreateJobRequest;
import com.taskforge.dto.JobResponse;
import com.taskforge.dto.PageResponse;
import com.taskforge.entity.JobEntity;
import com.taskforge.exception.JobConflictException;
import com.taskforge.exception.JobNotFoundException;
import com.taskforge.model.JobStatus;
import com.taskforge.repository.JobRepository;
import com.taskforge.validation.JobPayloadValidator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.dao.DataIntegrityViolationException;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.Pageable;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.time.Instant;
import java.util.Optional;
import java.util.UUID;

/**
 * Client-facing job operations: submit, read, list, cancel.
 *
 * <p>Execution concerns deliberately live elsewhere ({@code worker} and {@code execution}
 * packages); this class never runs a job or decides a retry.
 */
@Service
public class JobService {

    private static final Logger log = LoggerFactory.getLogger(JobService.class);

    private final JobRepository jobRepository;
    private final JobCreator jobCreator;
    private final JobMapper jobMapper;
    private final JobCache jobCache;
    private final JobPayloadValidator payloadValidator;

    public JobService(JobRepository jobRepository,
                      JobCreator jobCreator,
                      JobMapper jobMapper,
                      JobCache jobCache,
                      JobPayloadValidator payloadValidator) {
        this.jobRepository = jobRepository;
        this.jobCreator = jobCreator;
        this.jobMapper = jobMapper;
        this.jobCache = jobCache;
        this.payloadValidator = payloadValidator;
    }

    /**
     * Submits a job, or returns the existing one if this idempotency key was already used.
     *
     * <p>Idempotency is enforced by a unique index in PostgreSQL, not by the lookup below.
     * The lookup is only a fast path. When two identical requests race, both may pass it,
     * both may attempt the insert, and exactly one wins; the loser catches the constraint
     * violation and reads back the winner's row. That is why this method is not itself
     * transactional — see {@link JobCreator}.
     */
    public JobSubmissionResult submit(CreateJobRequest request) {
        String payloadJson = payloadValidator.validateAndSerialize(request.jobType(), request.payload());

        Optional<JobEntity> existing = jobRepository.findByIdempotencyKey(request.idempotencyKey());
        if (existing.isPresent()) {
            log.info("Job submission deduplicated idempotencyKey={} jobId={} status={}",
                    request.idempotencyKey(), existing.get().getId(), existing.get().getStatus());
            return new JobSubmissionResult(jobMapper.toResponse(existing.get()), false);
        }

        try {
            JobEntity created = jobCreator.createWithOutboxEvent(request, payloadJson);
            log.info("Job accepted jobId={} jobType={} idempotencyKey={} maxRetries={} timeoutSeconds={}",
                    created.getId(), created.getJobType(), created.getIdempotencyKey(),
                    created.getMaxRetries(), created.getTimeoutSeconds());
            return new JobSubmissionResult(jobMapper.toResponse(created), true);
        } catch (DataIntegrityViolationException e) {
            // Lost the race on the unique index. The winner's row is authoritative.
            JobEntity winner = jobRepository.findByIdempotencyKey(request.idempotencyKey())
                    .orElseThrow(() -> e);
            log.info("Concurrent submission collapsed onto jobId={} idempotencyKey={}",
                    winner.getId(), winner.getIdempotencyKey());
            return new JobSubmissionResult(jobMapper.toResponse(winner), false);
        }
    }

    /** Cache-aside read. PostgreSQL is consulted whenever Redis has nothing valid to offer. */
    @Transactional(readOnly = true)
    public JobResponse getJob(UUID jobId) {
        Optional<JobResponse> cached = jobCache.get(jobId);
        if (cached.isPresent()) {
            return cached.get();
        }

        JobResponse response = jobRepository.findById(jobId)
                .map(jobMapper::toResponse)
                .orElseThrow(() -> new JobNotFoundException(jobId));

        jobCache.put(response);
        return response;
    }

    /** List results are not cached: they would be invalidated by any job changing state. */
    @Transactional(readOnly = true)
    public PageResponse<JobResponse> listJobs(JobStatus status, Pageable pageable) {
        Page<JobEntity> page = status == null
                ? jobRepository.findAll(pageable)
                : jobRepository.findByStatus(status, pageable);
        return PageResponse.from(page, jobMapper::toResponse);
    }

    /**
     * Cancels a job, or asks a worker to stop it.
     *
     * <p>Both branches are single conditional UPDATEs whose WHERE clause carries the
     * precondition, so there is no window between checking the status and acting on it. A job
     * that completes microseconds before the request arrives simply matches neither statement
     * and is reported as a conflict.
     */
    @Transactional
    public JobResponse cancel(UUID jobId) {
        Instant now = Instant.now();

        if (jobRepository.cancelIfNotStarted(jobId, now) > 0) {
            jobCache.invalidate(jobId);
            log.info("Job cancelled before execution jobId={}", jobId);
            return reload(jobId);
        }

        if (jobRepository.requestCancelIfRunning(jobId, now) > 0) {
            jobCache.invalidate(jobId);
            log.info("Cancellation requested for running jobId={}", jobId);
            return reload(jobId);
        }

        JobEntity job = jobRepository.findById(jobId).orElseThrow(() -> new JobNotFoundException(jobId));

        // Already asked to stop: repeat calls are a no-op rather than an error.
        if (job.getStatus() == JobStatus.CANCEL_REQUESTED || job.getStatus() == JobStatus.CANCELLED) {
            return jobMapper.toResponse(job);
        }

        throw new JobConflictException(
                "Job %s is in status %s and can no longer be cancelled".formatted(jobId, job.getStatus()));
    }

    private JobResponse reload(UUID jobId) {
        return jobRepository.findById(jobId)
                .map(jobMapper::toResponse)
                .orElseThrow(() -> new JobNotFoundException(jobId));
    }
}
