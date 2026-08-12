CREATE TABLE outbox_events (
    id            UUID         PRIMARY KEY,
    aggregate_id  UUID         NOT NULL,
    event_type    VARCHAR(64)  NOT NULL,
    topic         VARCHAR(160) NOT NULL,
    payload       JSONB        NOT NULL,
    status        VARCHAR(16)  NOT NULL,
    attempt_count INTEGER      NOT NULL DEFAULT 0,
    last_error    TEXT,
    created_at    TIMESTAMPTZ  NOT NULL,
    published_at  TIMESTAMPTZ,

    CONSTRAINT ck_outbox_status CHECK (status IN ('PENDING', 'PUBLISHED'))
);

-- The publisher polls oldest-pending-first; the partial index keeps that scan
-- proportional to the backlog rather than to total history.
CREATE INDEX ix_outbox_pending ON outbox_events (created_at) WHERE status = 'PENDING';

CREATE INDEX ix_outbox_aggregate_id ON outbox_events (aggregate_id);
