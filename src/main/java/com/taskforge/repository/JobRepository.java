package com.taskforge.repository;

import com.taskforge.entity.JobEntity;
import com.taskforge.model.JobStatus;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.Pageable;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Modifying;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;
import org.springframework.stereotype.Repository;

import java.time.Instant;
import java.util.List;
import java.util.Optional;
import java.util.UUID;

/**
 * All job state transitions are expressed as conditional {@code UPDATE ... WHERE status = ?}
 * statements rather than read-modify-write through JPA.
 *
 * <p>This is the core concurrency guarantee of TaskForge: the {@code WHERE} clause is the
 * compare, the {@code SET} is the swap, and PostgreSQL performs both atomically under a row
 * lock. Every method returns the number of rows affected, so a caller can tell whether it
 * won the race. A return of {@code 0} always means "someone else got there first" — a
 * duplicate Kafka delivery, a competing worker, or a concurrent cancellation — and is never
 * an error on its own.
 */
@Repository
public interface JobRepository extends JpaRepository<JobEntity, UUID> {

    Optional<JobEntity> findByIdempotencyKey(String idempotencyKey);

    Page<JobEntity> findByStatus(JobStatus status, Pageable pageable);

    long countByStatus(JobStatus status);

    // ---------------------------------------------------------------------
    // Worker claim and lease
    // ---------------------------------------------------------------------

    /**
     * Atomically takes ownership of a QUEUED job. Exactly one concurrent caller can
     * observe a return value of 1; every other caller sees 0 and must not execute.
     */
    @Modifying(clearAutomatically = true, flushAutomatically = true)
    @Query(value = """
            UPDATE jobs
               SET status = 'RUNNING',
                   worker_id = :workerId,
                   started_at = :now,
                   updated_at = :now,
                   lease_expires_at = :leaseExpiresAt,
                   next_attempt_at = NULL,
                   error_message = NULL,
                   version = version + 1
             WHERE id = :id
               AND status = 'QUEUED'
            """, nativeQuery = true)
    int claim(@Param("id") UUID id,
              @Param("workerId") String workerId,
              @Param("now") Instant now,
              @Param("leaseExpiresAt") Instant leaseExpiresAt);

    /** Extends the lease of a job this worker still owns. Returns 0 once ownership is lost. */
    @Modifying(clearAutomatically = true, flushAutomatically = true)
    @Query(value = """
            UPDATE jobs
               SET lease_expires_at = :leaseExpiresAt,
                   updated_at = :now,
                   version = version + 1
             WHERE id = :id
               AND worker_id = :workerId
               AND status IN ('RUNNING', 'CANCEL_REQUESTED')
            """, nativeQuery = true)
    int renewLease(@Param("id") UUID id,
                   @Param("workerId") String workerId,
                   @Param("now") Instant now,
                   @Param("leaseExpiresAt") Instant leaseExpiresAt);

    // ---------------------------------------------------------------------
    // Worker-side terminal transitions (guarded by ownership)
    // ---------------------------------------------------------------------

    /**
     * CANCEL_REQUESTED is accepted here as well as RUNNING: a job can finish legitimately
     * in the window between a cancellation being requested and the interrupt landing.
     */
    @Modifying(clearAutomatically = true, flushAutomatically = true)
    @Query(value = """
            UPDATE jobs
               SET status = 'COMPLETED',
                   result = CAST(:result AS jsonb),
                   completed_at = :now,
                   updated_at = :now,
                   lease_expires_at = NULL,
                   error_message = NULL,
                   version = version + 1
             WHERE id = :id
               AND worker_id = :workerId
               AND status IN ('RUNNING', 'CANCEL_REQUESTED')
            """, nativeQuery = true)
    int markCompleted(@Param("id") UUID id,
                      @Param("workerId") String workerId,
                      @Param("result") String result,
                      @Param("now") Instant now);

    @Modifying(clearAutomatically = true, flushAutomatically = true)
    @Query(value = """
            UPDATE jobs
               SET status = 'RETRYING',
                   retry_count = retry_count + 1,
                   next_attempt_at = :nextAttemptAt,
                   error_message = :errorMessage,
                   updated_at = :now,
                   lease_expires_at = NULL,
                   version = version + 1
             WHERE id = :id
               AND worker_id = :workerId
               AND status IN ('RUNNING', 'CANCEL_REQUESTED')
            """, nativeQuery = true)
    int markRetrying(@Param("id") UUID id,
                     @Param("workerId") String workerId,
                     @Param("errorMessage") String errorMessage,
                     @Param("nextAttemptAt") Instant nextAttemptAt,
                     @Param("now") Instant now);

    @Modifying(clearAutomatically = true, flushAutomatically = true)
    @Query(value = """
            UPDATE jobs
               SET status = 'DEAD_LETTERED',
                   error_message = :errorMessage,
                   completed_at = :now,
                   updated_at = :now,
                   lease_expires_at = NULL,
                   version = version + 1
             WHERE id = :id
               AND worker_id = :workerId
               AND status IN ('RUNNING', 'CANCEL_REQUESTED')
            """, nativeQuery = true)
    int markDeadLettered(@Param("id") UUID id,
                         @Param("workerId") String workerId,
                         @Param("errorMessage") String errorMessage,
                         @Param("now") Instant now);

    @Modifying(clearAutomatically = true, flushAutomatically = true)
    @Query(value = """
            UPDATE jobs
               SET status = 'FAILED',
                   error_message = :errorMessage,
                   completed_at = :now,
                   updated_at = :now,
                   lease_expires_at = NULL,
                   version = version + 1
             WHERE id = :id
               AND worker_id = :workerId
               AND status IN ('RUNNING', 'CANCEL_REQUESTED')
            """, nativeQuery = true)
    int markFailed(@Param("id") UUID id,
                   @Param("workerId") String workerId,
                   @Param("errorMessage") String errorMessage,
                   @Param("now") Instant now);

    @Modifying(clearAutomatically = true, flushAutomatically = true)
    @Query(value = """
            UPDATE jobs
               SET status = 'CANCELLED',
                   error_message = :errorMessage,
                   completed_at = :now,
                   updated_at = :now,
                   lease_expires_at = NULL,
                   version = version + 1
             WHERE id = :id
               AND worker_id = :workerId
               AND status IN ('RUNNING', 'CANCEL_REQUESTED')
            """, nativeQuery = true)
    int markCancelledByWorker(@Param("id") UUID id,
                              @Param("workerId") String workerId,
                              @Param("errorMessage") String errorMessage,
                              @Param("now") Instant now);

    @Modifying(clearAutomatically = true, flushAutomatically = true)
    @Query(value = """
            UPDATE jobs
               SET status = 'TIMED_OUT',
                   error_message = :errorMessage,
                   updated_at = :now,
                   lease_expires_at = NULL,
                   version = version + 1
             WHERE id = :id
               AND worker_id = :workerId
               AND status IN ('RUNNING', 'CANCEL_REQUESTED')
            """, nativeQuery = true)
    int markTimedOutByWorker(@Param("id") UUID id,
                             @Param("workerId") String workerId,
                             @Param("errorMessage") String errorMessage,
                             @Param("now") Instant now);

    // ---------------------------------------------------------------------
    // API-side cancellation
    // ---------------------------------------------------------------------

    /** A job that is not yet executing can be cancelled outright. */
    @Modifying(clearAutomatically = true, flushAutomatically = true)
    @Query(value = """
            UPDATE jobs
               SET status = 'CANCELLED',
                   completed_at = :now,
                   updated_at = :now,
                   next_attempt_at = NULL,
                   error_message = 'Cancelled before execution started',
                   version = version + 1
             WHERE id = :id
               AND status IN ('QUEUED', 'RETRYING')
            """, nativeQuery = true)
    int cancelIfNotStarted(@Param("id") UUID id, @Param("now") Instant now);

    /** A running job can only be asked to stop; the owning worker completes the transition. */
    @Modifying(clearAutomatically = true, flushAutomatically = true)
    @Query(value = """
            UPDATE jobs
               SET status = 'CANCEL_REQUESTED',
                   updated_at = :now,
                   version = version + 1
             WHERE id = :id
               AND status = 'RUNNING'
            """, nativeQuery = true)
    int requestCancelIfRunning(@Param("id") UUID id, @Param("now") Instant now);

    // ---------------------------------------------------------------------
    // Retry scheduler
    // ---------------------------------------------------------------------

    /**
     * Locks a batch of retry-eligible jobs. {@code SKIP LOCKED} lets every application
     * instance run this scan concurrently while each row is handed to exactly one of them.
     */
    @Query(value = """
            SELECT id
              FROM jobs
             WHERE status = 'RETRYING'
               AND next_attempt_at <= :now
             ORDER BY next_attempt_at
             LIMIT :limit
               FOR UPDATE SKIP LOCKED
            """, nativeQuery = true)
    List<UUID> lockRetryableJobs(@Param("now") Instant now, @Param("limit") int limit);

    @Modifying(clearAutomatically = true, flushAutomatically = true)
    @Query(value = """
            UPDATE jobs
               SET status = 'QUEUED',
                   next_attempt_at = NULL,
                   worker_id = NULL,
                   started_at = NULL,
                   updated_at = :now,
                   version = version + 1
             WHERE id = :id
               AND status = 'RETRYING'
            """, nativeQuery = true)
    int requeueFromRetrying(@Param("id") UUID id, @Param("now") Instant now);

    // ---------------------------------------------------------------------
    // Abandoned-job recovery
    // ---------------------------------------------------------------------

    /**
     * Finds jobs whose owning worker has gone quiet: either the lease was not renewed, or
     * the job has been running past its own timeout. Rows are locked with
     * {@code SKIP LOCKED} so two recovery scans never adopt the same job.
     */
    @Query(value = """
            SELECT id
              FROM jobs
             WHERE status IN ('RUNNING', 'CANCEL_REQUESTED')
               AND (
                     (lease_expires_at IS NOT NULL AND lease_expires_at < :now)
                  OR (started_at IS NOT NULL
                      AND started_at + make_interval(secs => timeout_seconds) < :now)
                   )
             ORDER BY lease_expires_at
             LIMIT :limit
               FOR UPDATE SKIP LOCKED
            """, nativeQuery = true)
    List<UUID> lockAbandonedJobs(@Param("now") Instant now, @Param("limit") int limit);

    @Modifying(clearAutomatically = true, flushAutomatically = true)
    @Query(value = """
            UPDATE jobs
               SET status = 'TIMED_OUT',
                   error_message = :errorMessage,
                   updated_at = :now,
                   lease_expires_at = NULL,
                   version = version + 1
             WHERE id = :id
               AND status IN ('RUNNING', 'CANCEL_REQUESTED')
            """, nativeQuery = true)
    int markTimedOutByRecovery(@Param("id") UUID id,
                               @Param("errorMessage") String errorMessage,
                               @Param("now") Instant now);

    @Modifying(clearAutomatically = true, flushAutomatically = true)
    @Query(value = """
            UPDATE jobs
               SET status = 'RETRYING',
                   retry_count = retry_count + 1,
                   next_attempt_at = :nextAttemptAt,
                   updated_at = :now,
                   version = version + 1
             WHERE id = :id
               AND status = 'TIMED_OUT'
            """, nativeQuery = true)
    int retryFromTimedOut(@Param("id") UUID id,
                          @Param("nextAttemptAt") Instant nextAttemptAt,
                          @Param("now") Instant now);

    @Modifying(clearAutomatically = true, flushAutomatically = true)
    @Query(value = """
            UPDATE jobs
               SET status = 'DEAD_LETTERED',
                   error_message = :errorMessage,
                   completed_at = :now,
                   updated_at = :now,
                   version = version + 1
             WHERE id = :id
               AND status = 'TIMED_OUT'
            """, nativeQuery = true)
    int deadLetterFromTimedOut(@Param("id") UUID id,
                               @Param("errorMessage") String errorMessage,
                               @Param("now") Instant now);

    // ---------------------------------------------------------------------
    // Worker cancellation polling
    // ---------------------------------------------------------------------

    @Query(value = """
            SELECT id
              FROM jobs
             WHERE worker_id = :workerId
               AND status = 'CANCEL_REQUESTED'
            """, nativeQuery = true)
    List<UUID> findCancelRequestedForWorker(@Param("workerId") String workerId);
}
