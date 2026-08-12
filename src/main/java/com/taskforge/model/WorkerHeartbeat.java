package com.taskforge.model;

import java.time.Instant;

/**
 * Liveness signal a worker writes to Redis under a TTL.
 *
 * <p>Deliberately not persisted to PostgreSQL: this is ephemeral operational data, and a
 * worker that stops writing it should simply disappear rather than leave a row behind that
 * someone has to clean up. Absence of the key is the signal.
 */
public record WorkerHeartbeat(
        String workerId,
        Instant startedAt,
        Instant lastHeartbeat,
        String status,
        int activeJobs,
        int threadCount) {
}
