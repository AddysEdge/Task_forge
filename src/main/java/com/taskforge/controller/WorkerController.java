package com.taskforge.controller;

import com.taskforge.cache.WorkerHeartbeatStore;
import com.taskforge.model.WorkerHeartbeat;
import org.springframework.context.annotation.Profile;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;

/**
 * Read-only view of which workers are currently alive, straight from the Redis heartbeat
 * keys. Handy for confirming that jobs really are spread across separate processes.
 *
 * <p>This reflects liveness, not job ownership: a job's authoritative owner is the
 * {@code worker_id} column in PostgreSQL.
 */
@RestController
@RequestMapping("/api/v1/workers")
@Profile({"api", "all"})
public class WorkerController {

    private final WorkerHeartbeatStore heartbeatStore;

    public WorkerController(WorkerHeartbeatStore heartbeatStore) {
        this.heartbeatStore = heartbeatStore;
    }

    @GetMapping
    public List<WorkerHeartbeat> listWorkers() {
        return heartbeatStore.listWorkers();
    }
}
