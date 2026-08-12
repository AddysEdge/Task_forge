package com.taskforge.worker;

import com.taskforge.cache.WorkerHeartbeatStore;
import com.taskforge.config.TaskForgeProperties;
import com.taskforge.config.WorkerIdentity;
import com.taskforge.model.WorkerHeartbeat;
import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.annotation.Profile;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.time.Instant;

/**
 * Publishes this worker's liveness to Redis under a TTL a few beats long.
 *
 * <p>Because the key expires on its own, a worker that is killed, partitioned or frozen
 * stops being visible without anything having to notice or clean up after it. That absence
 * is the signal {@link com.taskforge.recovery.JobRecoveryService} uses to distinguish a
 * genuinely dead worker from a merely slow one.
 */
@Component
@Profile({"worker", "all"})
public class WorkerHeartbeatPublisher {

    private static final Logger log = LoggerFactory.getLogger(WorkerHeartbeatPublisher.class);

    private final WorkerHeartbeatStore heartbeatStore;
    private final WorkerIdentity workerIdentity;
    private final WorkerJobExecutor workerJobExecutor;
    private final TaskForgeProperties properties;
    private final Instant startedAt = Instant.now();

    public WorkerHeartbeatPublisher(WorkerHeartbeatStore heartbeatStore,
                                    WorkerIdentity workerIdentity,
                                    WorkerJobExecutor workerJobExecutor,
                                    TaskForgeProperties properties) {
        this.heartbeatStore = heartbeatStore;
        this.workerIdentity = workerIdentity;
        this.workerJobExecutor = workerJobExecutor;
        this.properties = properties;
    }

    /** Announce immediately, rather than after one full interval of invisibility. */
    @PostConstruct
    public void publishInitialHeartbeat() {
        publishHeartbeat();
        log.info("Worker registered workerId={} threadCount={}",
                workerIdentity.workerId(), properties.getWorker().getThreadCount());
    }

    @Scheduled(fixedDelayString = "${taskforge.worker.heartbeat-interval:10000}")
    public void publishHeartbeat() {
        heartbeatStore.publish(
                new WorkerHeartbeat(
                        workerIdentity.workerId(),
                        startedAt,
                        Instant.now(),
                        "UP",
                        workerJobExecutor.activeCount(),
                        properties.getWorker().getThreadCount()),
                properties.getWorker().getHeartbeatTtl());
    }

    /** On a clean shutdown, disappear at once instead of waiting out the TTL. */
    @PreDestroy
    public void deregister() {
        log.info("Worker deregistering workerId={}", workerIdentity.workerId());
        heartbeatStore.remove(workerIdentity.workerId());
    }
}
