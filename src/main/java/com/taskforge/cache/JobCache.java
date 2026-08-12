package com.taskforge.cache;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.taskforge.config.TaskForgeProperties;
import com.taskforge.dto.JobResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.dao.DataAccessException;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Component;
import org.springframework.transaction.support.TransactionSynchronization;
import org.springframework.transaction.support.TransactionSynchronizationManager;

import java.util.Optional;
import java.util.UUID;

/**
 * Read-through cache of job state, keyed {@code job:{jobId}}.
 *
 * <p>Two rules keep this from ever contradicting PostgreSQL:
 *
 * <ol>
 *   <li><b>Invalidate, never update.</b> A state change deletes the key rather than writing
 *       the new value, so an out-of-order write from a slower thread cannot resurrect an
 *       older state.</li>
 *   <li><b>Invalidate after commit.</b> Deleting before the transaction commits would let a
 *       concurrent reader repopulate the key from the pre-commit row, leaving the cache
 *       stale until its TTL. When a transaction is active the delete is deferred to
 *       after-commit; otherwise it happens immediately.</li>
 * </ol>
 *
 * <p>Redis being unavailable degrades reads to PostgreSQL rather than failing them — the
 * failure is logged, not swallowed.
 */
@Component
public class JobCache {

    private static final Logger log = LoggerFactory.getLogger(JobCache.class);
    private static final String KEY_PREFIX = "job:";

    private final StringRedisTemplate redis;
    private final ObjectMapper objectMapper;
    private final TaskForgeProperties properties;

    public JobCache(StringRedisTemplate redis, ObjectMapper objectMapper, TaskForgeProperties properties) {
        this.redis = redis;
        this.objectMapper = objectMapper;
        this.properties = properties;
    }

    public Optional<JobResponse> get(UUID jobId) {
        if (!properties.getCache().isEnabled()) {
            return Optional.empty();
        }
        try {
            String raw = redis.opsForValue().get(key(jobId));
            if (raw == null) {
                return Optional.empty();
            }
            return Optional.of(objectMapper.readValue(raw, JobResponse.class));
        } catch (JsonProcessingException e) {
            // A cached entry we can no longer parse (e.g. after a DTO change) is not fatal:
            // drop it and fall back to the database.
            log.warn("Discarding unreadable cache entry for jobId={}: {}", jobId, e.getMessage());
            invalidateNow(jobId);
            return Optional.empty();
        } catch (DataAccessException e) {
            log.warn("Redis unavailable for jobId={} read, falling back to PostgreSQL: {}", jobId, e.getMessage());
            return Optional.empty();
        }
    }

    public void put(JobResponse job) {
        if (!properties.getCache().isEnabled()) {
            return;
        }
        try {
            redis.opsForValue().set(
                    key(job.id()),
                    objectMapper.writeValueAsString(job),
                    properties.getCache().getJobTtl());
        } catch (JsonProcessingException e) {
            log.warn("Could not serialise jobId={} for caching: {}", job.id(), e.getMessage());
        } catch (DataAccessException e) {
            log.warn("Redis unavailable for jobId={} write: {}", job.id(), e.getMessage());
        }
    }

    /**
     * Drops the cached view of a job. If called inside a transaction the delete is deferred
     * until that transaction commits, so readers cannot repopulate stale data in between.
     */
    public void invalidate(UUID jobId) {
        if (!properties.getCache().isEnabled()) {
            return;
        }
        if (TransactionSynchronizationManager.isSynchronizationActive()) {
            TransactionSynchronizationManager.registerSynchronization(new TransactionSynchronization() {
                @Override
                public void afterCompletion(int status) {
                    // Invalidate on rollback too: the only cost is a cache miss, and it
                    // protects against a partially applied change having been read.
                    invalidateNow(jobId);
                }
            });
        } else {
            invalidateNow(jobId);
        }
    }

    private void invalidateNow(UUID jobId) {
        try {
            redis.delete(key(jobId));
        } catch (DataAccessException e) {
            // The TTL is the backstop: a missed invalidation self-corrects within jobTtl.
            log.warn("Redis unavailable while invalidating jobId={}: {}", jobId, e.getMessage());
        }
    }

    private String key(UUID jobId) {
        return KEY_PREFIX + jobId;
    }
}
