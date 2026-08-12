package com.taskforge.entity;

import com.taskforge.model.JobStatus;
import com.taskforge.model.JobType;
import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.EnumType;
import jakarta.persistence.Enumerated;
import jakarta.persistence.Id;
import jakarta.persistence.Table;
import jakarta.persistence.Version;
import org.hibernate.annotations.JdbcTypeCode;
import org.hibernate.type.SqlTypes;

import java.time.Instant;
import java.util.UUID;

/**
 * Durable job record. PostgreSQL is the authoritative source of truth for every
 * field here; Redis only ever holds a derived, expendable copy.
 *
 * <p>State changes are applied through conditional {@code UPDATE ... WHERE status = ?}
 * statements in {@link com.taskforge.repository.JobRepository} rather than through JPA
 * dirty checking, so that two workers racing on the same job cannot both win. The
 * {@link #version} column is incremented by those statements and exposed to clients.
 */
@Entity
@Table(name = "jobs")
public class JobEntity {

    @Id
    @Column(name = "id", nullable = false, updatable = false)
    private UUID id;

    @Column(name = "idempotency_key", nullable = false, updatable = false, length = 200)
    private String idempotencyKey;

    @Enumerated(EnumType.STRING)
    @Column(name = "job_type", nullable = false, updatable = false, length = 32)
    private JobType jobType;

    @JdbcTypeCode(SqlTypes.JSON)
    @Column(name = "payload", nullable = false)
    private String payload;

    @Enumerated(EnumType.STRING)
    @Column(name = "status", nullable = false, length = 32)
    private JobStatus status;

    @Column(name = "created_at", nullable = false, updatable = false)
    private Instant createdAt;

    @Column(name = "updated_at", nullable = false)
    private Instant updatedAt;

    @Column(name = "started_at")
    private Instant startedAt;

    @Column(name = "completed_at")
    private Instant completedAt;

    @Column(name = "retry_count", nullable = false)
    private int retryCount;

    @Column(name = "max_retries", nullable = false)
    private int maxRetries;

    @Column(name = "timeout_seconds", nullable = false)
    private int timeoutSeconds;

    /** When a RETRYING job becomes eligible to be re-queued. Null in every other state. */
    @Column(name = "next_attempt_at")
    private Instant nextAttemptAt;

    /** Identity of the worker process currently holding, or last holding, the job. */
    @Column(name = "worker_id", length = 120)
    private String workerId;

    /** Deadline by which the owning worker must renew, or the job is considered abandoned. */
    @Column(name = "lease_expires_at")
    private Instant leaseExpiresAt;

    @JdbcTypeCode(SqlTypes.JSON)
    @Column(name = "result")
    private String result;

    @Column(name = "error_message")
    private String errorMessage;

    @Version
    @Column(name = "version", nullable = false)
    private long version;

    protected JobEntity() {
        // for JPA
    }

    private JobEntity(Builder builder) {
        this.id = builder.id;
        this.idempotencyKey = builder.idempotencyKey;
        this.jobType = builder.jobType;
        this.payload = builder.payload;
        this.status = builder.status;
        this.createdAt = builder.createdAt;
        this.updatedAt = builder.updatedAt;
        this.retryCount = builder.retryCount;
        this.maxRetries = builder.maxRetries;
        this.timeoutSeconds = builder.timeoutSeconds;
        this.startedAt = builder.startedAt;
        this.workerId = builder.workerId;
        this.leaseExpiresAt = builder.leaseExpiresAt;
        this.nextAttemptAt = builder.nextAttemptAt;
    }

    public static Builder builder() {
        return new Builder();
    }

    public UUID getId() {
        return id;
    }

    public String getIdempotencyKey() {
        return idempotencyKey;
    }

    public JobType getJobType() {
        return jobType;
    }

    public String getPayload() {
        return payload;
    }

    public JobStatus getStatus() {
        return status;
    }

    public Instant getCreatedAt() {
        return createdAt;
    }

    public Instant getUpdatedAt() {
        return updatedAt;
    }

    public Instant getStartedAt() {
        return startedAt;
    }

    public Instant getCompletedAt() {
        return completedAt;
    }

    public int getRetryCount() {
        return retryCount;
    }

    public int getMaxRetries() {
        return maxRetries;
    }

    public int getTimeoutSeconds() {
        return timeoutSeconds;
    }

    public Instant getNextAttemptAt() {
        return nextAttemptAt;
    }

    public String getWorkerId() {
        return workerId;
    }

    public Instant getLeaseExpiresAt() {
        return leaseExpiresAt;
    }

    public String getResult() {
        return result;
    }

    public String getErrorMessage() {
        return errorMessage;
    }

    public long getVersion() {
        return version;
    }

    /** The attempt number a worker claiming this job right now would be executing. */
    public int currentAttempt() {
        return retryCount + 1;
    }

    public static final class Builder {
        private UUID id = UUID.randomUUID();
        private String idempotencyKey;
        private JobType jobType;
        private String payload;
        private JobStatus status = JobStatus.QUEUED;
        private Instant createdAt = Instant.now();
        private Instant updatedAt = Instant.now();
        private int retryCount;
        private int maxRetries;
        private int timeoutSeconds;
        private Instant startedAt;
        private String workerId;
        private Instant leaseExpiresAt;
        private Instant nextAttemptAt;

        public Builder id(UUID value) {
            this.id = value;
            return this;
        }

        public Builder idempotencyKey(String value) {
            this.idempotencyKey = value;
            return this;
        }

        public Builder jobType(JobType value) {
            this.jobType = value;
            return this;
        }

        public Builder payload(String value) {
            this.payload = value;
            return this;
        }

        public Builder status(JobStatus value) {
            this.status = value;
            return this;
        }

        public Builder createdAt(Instant value) {
            this.createdAt = value;
            return this;
        }

        public Builder updatedAt(Instant value) {
            this.updatedAt = value;
            return this;
        }

        public Builder retryCount(int value) {
            this.retryCount = value;
            return this;
        }

        public Builder maxRetries(int value) {
            this.maxRetries = value;
            return this;
        }

        public Builder timeoutSeconds(int value) {
            this.timeoutSeconds = value;
            return this;
        }

        public Builder startedAt(Instant value) {
            this.startedAt = value;
            return this;
        }

        public Builder workerId(String value) {
            this.workerId = value;
            return this;
        }

        public Builder leaseExpiresAt(Instant value) {
            this.leaseExpiresAt = value;
            return this;
        }

        public Builder nextAttemptAt(Instant value) {
            this.nextAttemptAt = value;
            return this;
        }

        public JobEntity build() {
            return new JobEntity(this);
        }
    }
}
