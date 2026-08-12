package com.taskforge.worker;

import com.taskforge.execution.JobExecutionContext;
import org.springframework.stereotype.Component;

import java.util.Collection;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

/**
 * The jobs this process is executing right now.
 *
 * <p>Three background tasks need this map — the lease renewer, the cancellation poller and
 * the timeout watchdog — and all of them run on scheduler threads while handlers run on
 * pool threads, so it is a {@link ConcurrentHashMap} and the contexts it holds are
 * internally thread-safe.
 *
 * <p>It is process-local by design. Authoritative ownership lives in the {@code worker_id}
 * and {@code lease_expires_at} columns; losing this map on a crash costs nothing, because
 * recovery reconstructs what was abandoned from the database.
 */
@Component
public class RunningJobRegistry {

    private final Map<UUID, JobExecutionContext> running = new ConcurrentHashMap<>();

    public void register(JobExecutionContext context) {
        running.put(context.jobId(), context);
    }

    public void unregister(UUID jobId) {
        running.remove(jobId);
    }

    public Optional<JobExecutionContext> find(UUID jobId) {
        return Optional.ofNullable(running.get(jobId));
    }

    public Set<UUID> runningJobIds() {
        return Set.copyOf(running.keySet());
    }

    public Collection<JobExecutionContext> contexts() {
        return running.values();
    }

    public int size() {
        return running.size();
    }
}
