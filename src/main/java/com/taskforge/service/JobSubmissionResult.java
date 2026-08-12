package com.taskforge.service;

import com.taskforge.dto.JobResponse;

/**
 * Outcome of a submission.
 *
 * @param job     the job the caller should observe, whether it created it or not
 * @param created false when an existing job was returned for a repeated idempotency key,
 *                which the controller turns into HTTP 200 rather than 201
 */
public record JobSubmissionResult(JobResponse job, boolean created) {
}
