package com.taskforge.config;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;
import org.springframework.util.StringUtils;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.UUID;

/**
 * The identity a worker process writes into {@code jobs.worker_id} and its heartbeat key.
 *
 * <p>Uniqueness matters for correctness, not just for logs: job ownership checks are
 * expressed as {@code WHERE worker_id = ?}, so two processes sharing an id could each
 * believe they own the other's work. Docker Compose sets {@code WORKER_ID} explicitly; the
 * generated fallback appends a random suffix to the hostname so that two containers from the
 * same image still differ.
 */
@Component
public class WorkerIdentity {

    private static final Logger log = LoggerFactory.getLogger(WorkerIdentity.class);

    private final String workerId;

    public WorkerIdentity(TaskForgeProperties properties) {
        String configured = properties.getWorker().getId();
        this.workerId = StringUtils.hasText(configured) ? configured.trim() : generateId();
        log.info("Worker identity resolved to workerId={}", workerId);
    }

    private String generateId() {
        String host;
        try {
            host = InetAddress.getLocalHost().getHostName();
        } catch (UnknownHostException e) {
            host = "unknown-host";
        }
        return host + "-" + UUID.randomUUID().toString().substring(0, 8);
    }

    public String workerId() {
        return workerId;
    }
}
