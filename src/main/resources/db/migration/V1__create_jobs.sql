CREATE TABLE jobs (
    id                UUID         PRIMARY KEY,
    idempotency_key   VARCHAR(200) NOT NULL,
    job_type          VARCHAR(32)  NOT NULL,
    payload           JSONB        NOT NULL,
    status            VARCHAR(32)  NOT NULL,
    created_at        TIMESTAMPTZ  NOT NULL,
    updated_at        TIMESTAMPTZ  NOT NULL,
    started_at        TIMESTAMPTZ,
    completed_at      TIMESTAMPTZ,
    retry_count       INTEGER      NOT NULL DEFAULT 0,
    max_retries       INTEGER      NOT NULL,
    timeout_seconds   INTEGER      NOT NULL,
    next_attempt_at   TIMESTAMPTZ,
    worker_id         VARCHAR(120),
    lease_expires_at  TIMESTAMPTZ,
    result            JSONB,
    error_message     TEXT,
    version           BIGINT       NOT NULL DEFAULT 0,

    CONSTRAINT ck_jobs_retry_count_non_negative CHECK (retry_count >= 0),
    CONSTRAINT ck_jobs_max_retries_range        CHECK (max_retries BETWEEN 0 AND 10),
    CONSTRAINT ck_jobs_timeout_range            CHECK (timeout_seconds BETWEEN 1 AND 3600)
);

-- Idempotency is enforced by the database, not by application-level checks.
CREATE UNIQUE INDEX ux_jobs_idempotency_key ON jobs (idempotency_key);

-- Listing endpoint: newest-first, optionally filtered by status.
CREATE INDEX ix_jobs_created_at ON jobs (created_at DESC);
CREATE INDEX ix_jobs_status_created_at ON jobs (status, created_at DESC);

-- Recovery scans: only RUNNING rows are ever considered.
CREATE INDEX ix_jobs_lease_expires_at ON jobs (lease_expires_at) WHERE status = 'RUNNING';

-- Retry scheduler scans: only RETRYING rows are ever considered.
CREATE INDEX ix_jobs_next_attempt_at ON jobs (next_attempt_at) WHERE status = 'RETRYING';

-- "Which jobs did worker X run?" and stale-worker investigation.
CREATE INDEX ix_jobs_worker_id ON jobs (worker_id);
