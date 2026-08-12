package com.taskforge.worker;

import com.taskforge.config.TaskForgeProperties;
import jakarta.annotation.PreDestroy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.annotation.Profile;
import org.springframework.stereotype.Component;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * The bounded pool that runs job handlers. Nothing in TaskForge starts a bare thread.
 *
 * <p>The interesting part is what happens when the queue fills. Rather than dropping the
 * task or growing without limit, the rejection handler blocks the submitting thread — which
 * is the Kafka listener thread — until space frees up. Consumption then simply slows down,
 * the broker's lag grows, and no job is lost. That is why {@code max.poll.interval.ms} is
 * configured generously: a listener parked on backpressure must not be mistaken for a dead
 * consumer and trigger a rebalance.
 *
 * <p>This is backpressure, not a retry delay. Retry waits are never served by blocking a
 * consumer thread; they are persisted as {@code next_attempt_at} timestamps.
 */
@Component
@Profile({"worker", "all"})
public class WorkerJobExecutor {

    private static final Logger log = LoggerFactory.getLogger(WorkerJobExecutor.class);
    private static final long SHUTDOWN_GRACE_SECONDS = 30;

    private final ThreadPoolExecutor executor;
    private final BlockingQueue<Runnable> queue;

    public WorkerJobExecutor(TaskForgeProperties properties) {
        int threads = properties.getWorker().getThreadCount();
        this.queue = new LinkedBlockingQueue<>(properties.getWorker().getQueueCapacity());
        this.executor = new ThreadPoolExecutor(
                threads, threads,
                0L, TimeUnit.MILLISECONDS,
                queue,
                namedThreadFactory(),
                this::blockUntilCapacityAvailable);

        log.info("Worker executor started with threadCount={} queueCapacity={}",
                threads, properties.getWorker().getQueueCapacity());
    }

    public void submit(Runnable task) {
        executor.execute(task);
    }

    /** Jobs currently occupying a thread, used for heartbeat reporting. */
    public int activeCount() {
        return executor.getActiveCount();
    }

    public int queuedCount() {
        return queue.size();
    }

    private void blockUntilCapacityAvailable(Runnable task, ThreadPoolExecutor pool) {
        if (pool.isShutdown()) {
            throw new RejectedExecutionException("Worker executor is shutting down");
        }
        try {
            log.debug("Worker queue is full; applying backpressure to the submitting thread");
            pool.getQueue().put(task);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RejectedExecutionException("Interrupted while waiting for worker capacity", e);
        }
    }

    private ThreadFactory namedThreadFactory() {
        AtomicInteger counter = new AtomicInteger(1);
        return runnable -> {
            Thread thread = new Thread(runnable, "taskforge-job-" + counter.getAndIncrement());
            thread.setDaemon(false);
            return thread;
        };
    }

    /**
     * Stops accepting work and gives running handlers a chance to finish. Anything still
     * running after the grace period is interrupted; because handlers are cooperative, they
     * unwind and their jobs are left for recovery rather than being marked failed.
     */
    @PreDestroy
    public void shutdown() {
        log.info("Shutting down worker executor: {} active, {} queued", activeCount(), queuedCount());
        executor.shutdown();
        try {
            if (!executor.awaitTermination(SHUTDOWN_GRACE_SECONDS, TimeUnit.SECONDS)) {
                log.warn("Worker executor did not drain within {}s; interrupting remaining jobs",
                        SHUTDOWN_GRACE_SECONDS);
                executor.shutdownNow();
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            executor.shutdownNow();
        }
    }
}
