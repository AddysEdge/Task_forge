package com.taskforge.cache;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.taskforge.model.WorkerHeartbeat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.dao.DataAccessException;
import org.springframework.data.redis.core.Cursor;
import org.springframework.data.redis.core.ScanOptions;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Component;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

/**
 * Worker liveness in Redis under {@code worker:{workerId}:heartbeat}, expiring by TTL.
 *
 * <p>Recovery treats a missing key as "this worker is gone". That is a hint, not a proof —
 * it is combined with the job's database lease, which is the authoritative signal. A false
 * "worker is dead" reading can therefore only ever make recovery act sooner, never make it
 * steal a job whose lease is still valid.
 */
@Component
public class WorkerHeartbeatStore {

    private static final Logger log = LoggerFactory.getLogger(WorkerHeartbeatStore.class);
    private static final String KEY_PREFIX = "worker:";
    private static final String KEY_SUFFIX = ":heartbeat";
    private static final String KEY_PATTERN = KEY_PREFIX + "*" + KEY_SUFFIX;

    private final StringRedisTemplate redis;
    private final ObjectMapper objectMapper;

    public WorkerHeartbeatStore(StringRedisTemplate redis, ObjectMapper objectMapper) {
        this.redis = redis;
        this.objectMapper = objectMapper;
    }

    public void publish(WorkerHeartbeat heartbeat, Duration ttl) {
        try {
            redis.opsForValue().set(key(heartbeat.workerId()), objectMapper.writeValueAsString(heartbeat), ttl);
        } catch (JsonProcessingException e) {
            log.warn("Could not serialise heartbeat for workerId={}: {}", heartbeat.workerId(), e.getMessage());
        } catch (DataAccessException e) {
            log.warn("Redis unavailable while publishing heartbeat for workerId={}: {}",
                    heartbeat.workerId(), e.getMessage());
        }
    }

    /**
     * @return true if the worker has a live heartbeat key. Returns true when Redis itself is
     *         unreachable, so that an outage of the cache cannot cause mass job recovery.
     */
    public boolean isAlive(String workerId) {
        if (workerId == null) {
            return false;
        }
        try {
            return Boolean.TRUE.equals(redis.hasKey(key(workerId)));
        } catch (DataAccessException e) {
            log.warn("Redis unavailable while checking liveness of workerId={}; assuming alive: {}",
                    workerId, e.getMessage());
            return true;
        }
    }

    /** Removes the key on a clean shutdown, so the worker disappears at once rather than at TTL. */
    public void remove(String workerId) {
        try {
            redis.delete(key(workerId));
        } catch (DataAccessException e) {
            log.warn("Redis unavailable while removing heartbeat for workerId={}: {}", workerId, e.getMessage());
        }
    }

    public Optional<WorkerHeartbeat> find(String workerId) {
        try {
            String raw = redis.opsForValue().get(key(workerId));
            return raw == null ? Optional.empty() : Optional.of(objectMapper.readValue(raw, WorkerHeartbeat.class));
        } catch (JsonProcessingException | DataAccessException e) {
            log.warn("Could not read heartbeat for workerId={}: {}", workerId, e.getMessage());
            return Optional.empty();
        }
    }

    /** SCAN rather than KEYS, so listing workers never blocks the Redis event loop. */
    public List<WorkerHeartbeat> listWorkers() {
        List<WorkerHeartbeat> workers = new ArrayList<>();
        try (Cursor<String> cursor = redis.scan(ScanOptions.scanOptions().match(KEY_PATTERN).count(100).build())) {
            while (cursor.hasNext()) {
                String raw = redis.opsForValue().get(cursor.next());
                if (raw != null) {
                    workers.add(objectMapper.readValue(raw, WorkerHeartbeat.class));
                }
            }
        } catch (JsonProcessingException | DataAccessException e) {
            log.warn("Could not list worker heartbeats: {}", e.getMessage());
        }
        return workers;
    }

    private String key(String workerId) {
        return KEY_PREFIX + workerId + KEY_SUFFIX;
    }
}
