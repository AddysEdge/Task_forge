package com.taskforge.controller;

import com.taskforge.dto.CreateJobRequest;
import com.taskforge.dto.JobResponse;
import com.taskforge.dto.PageResponse;
import com.taskforge.model.JobStatus;
import com.taskforge.service.JobService;
import com.taskforge.service.JobSubmissionResult;
import jakarta.validation.Valid;
import jakarta.validation.constraints.Max;
import jakarta.validation.constraints.Min;
import org.springframework.context.annotation.Profile;
import org.springframework.data.domain.PageRequest;
import org.springframework.data.domain.Sort;
import org.springframework.http.ResponseEntity;
import org.springframework.validation.annotation.Validated;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.util.UriComponentsBuilder;

import java.net.URI;
import java.util.UUID;

/**
 * Versioned job API. Only present when the process runs with the {@code api} role.
 */
@RestController
@RequestMapping("/api/v1/jobs")
@Validated
@Profile({"api", "all"})
public class JobController {

    private static final int MAX_PAGE_SIZE = 200;

    private final JobService jobService;

    public JobController(JobService jobService) {
        this.jobService = jobService;
    }

    /**
     * Submits a job.
     *
     * <p>Returns 201 with a {@code Location} header for a newly created job, and 200 when the
     * idempotency key has been seen before — the caller gets the same job either way, and can
     * tell from the status code whether this particular request was the one that created it.
     */
    @PostMapping
    public ResponseEntity<JobResponse> submit(@Valid @RequestBody CreateJobRequest request) {
        JobSubmissionResult result = jobService.submit(request);

        if (!result.created()) {
            return ResponseEntity.ok(result.job());
        }

        URI location = UriComponentsBuilder.fromPath("/api/v1/jobs/{jobId}")
                .buildAndExpand(result.job().id())
                .toUri();
        return ResponseEntity.created(location).body(result.job());
    }

    @GetMapping("/{jobId}")
    public JobResponse get(@PathVariable UUID jobId) {
        return jobService.getJob(jobId);
    }

    /** Newest first, optionally narrowed to a single status. */
    @GetMapping
    public PageResponse<JobResponse> list(
            @RequestParam(required = false) JobStatus status,
            @RequestParam(defaultValue = "0") @Min(0) int page,
            @RequestParam(defaultValue = "20") @Min(1) @Max(MAX_PAGE_SIZE) int size) {

        return jobService.listJobs(status,
                PageRequest.of(page, size, Sort.by(Sort.Direction.DESC, "createdAt")));
    }

    /**
     * Requests cancellation. A job that has not started is cancelled outright; a running job
     * is moved to CANCEL_REQUESTED and its worker is signalled. Cancellation of running work
     * is cooperative and therefore best-effort — see the README.
     */
    @PostMapping("/{jobId}/cancel")
    public JobResponse cancel(@PathVariable UUID jobId) {
        return jobService.cancel(jobId);
    }
}
