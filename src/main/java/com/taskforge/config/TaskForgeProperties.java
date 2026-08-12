package com.taskforge.config;

import org.springframework.boot.context.properties.ConfigurationProperties;

import java.time.Duration;

/**
 * Every tunable knob in TaskForge, bound from {@code application.yml} and overridable by
 * environment variables. Defaults here are the ones used by the local Docker environment.
 */
@ConfigurationProperties(prefix = "taskforge")
public class TaskForgeProperties {

    private final Worker worker = new Worker();
    private final Retry retry = new Retry();
    private final Outbox outbox = new Outbox();
    private final Recovery recovery = new Recovery();
    private final Cache cache = new Cache();
    private final Topics topics = new Topics();
    private final Limits limits = new Limits();

    public Worker getWorker() {
        return worker;
    }

    public Retry getRetry() {
        return retry;
    }

    public Outbox getOutbox() {
        return outbox;
    }

    public Recovery getRecovery() {
        return recovery;
    }

    public Cache getCache() {
        return cache;
    }

    public Topics getTopics() {
        return topics;
    }

    public Limits getLimits() {
        return limits;
    }

    public static class Worker {

        /** Blank means "derive from hostname plus a random suffix" at startup. */
        private String id = "";

        /** Size of the fixed executor that runs job handlers. */
        private int threadCount = 4;

        /** Bounded backlog. When full, the Kafka listener thread blocks, applying backpressure. */
        private int queueCapacity = 100;

        /** How long a claim stays valid without renewal before recovery may adopt the job. */
        private Duration leaseDuration = Duration.ofSeconds(60);

        /** Must be comfortably shorter than {@link #leaseDuration}. */
        private Duration leaseRenewalInterval = Duration.ofSeconds(15);

        private Duration heartbeatInterval = Duration.ofSeconds(10);

        /** Redis TTL on the heartbeat key. Longer than the interval to tolerate a missed beat. */
        private Duration heartbeatTtl = Duration.ofSeconds(30);

        /** How often a worker looks for cancellation requests against jobs it owns. */
        private Duration cancellationPollInterval = Duration.ofSeconds(2);

        /** How often the in-process watchdog checks running jobs against their deadlines. */
        private Duration timeoutCheckInterval = Duration.ofSeconds(1);

        public String getId() {
            return id;
        }

        public void setId(String id) {
            this.id = id;
        }

        public int getThreadCount() {
            return threadCount;
        }

        public void setThreadCount(int threadCount) {
            this.threadCount = threadCount;
        }

        public int getQueueCapacity() {
            return queueCapacity;
        }

        public void setQueueCapacity(int queueCapacity) {
            this.queueCapacity = queueCapacity;
        }

        public Duration getLeaseDuration() {
            return leaseDuration;
        }

        public void setLeaseDuration(Duration leaseDuration) {
            this.leaseDuration = leaseDuration;
        }

        public Duration getLeaseRenewalInterval() {
            return leaseRenewalInterval;
        }

        public void setLeaseRenewalInterval(Duration leaseRenewalInterval) {
            this.leaseRenewalInterval = leaseRenewalInterval;
        }

        public Duration getHeartbeatInterval() {
            return heartbeatInterval;
        }

        public void setHeartbeatInterval(Duration heartbeatInterval) {
            this.heartbeatInterval = heartbeatInterval;
        }

        public Duration getHeartbeatTtl() {
            return heartbeatTtl;
        }

        public void setHeartbeatTtl(Duration heartbeatTtl) {
            this.heartbeatTtl = heartbeatTtl;
        }

        public Duration getCancellationPollInterval() {
            return cancellationPollInterval;
        }

        public void setCancellationPollInterval(Duration cancellationPollInterval) {
            this.cancellationPollInterval = cancellationPollInterval;
        }

        public Duration getTimeoutCheckInterval() {
            return timeoutCheckInterval;
        }

        public void setTimeoutCheckInterval(Duration timeoutCheckInterval) {
            this.timeoutCheckInterval = timeoutCheckInterval;
        }
    }

    public static class Retry {

        private Duration initialDelay = Duration.ofSeconds(1);
        private double multiplier = 2.0;
        private Duration maxDelay = Duration.ofSeconds(60);
        private int defaultMaxRetries = 3;

        /** How often RETRYING jobs whose backoff has elapsed are re-queued. */
        private Duration schedulerInterval = Duration.ofSeconds(1);
        private int batchSize = 100;
        private boolean enabled = true;

        public Duration getInitialDelay() {
            return initialDelay;
        }

        public void setInitialDelay(Duration initialDelay) {
            this.initialDelay = initialDelay;
        }

        public double getMultiplier() {
            return multiplier;
        }

        public void setMultiplier(double multiplier) {
            this.multiplier = multiplier;
        }

        public Duration getMaxDelay() {
            return maxDelay;
        }

        public void setMaxDelay(Duration maxDelay) {
            this.maxDelay = maxDelay;
        }

        public int getDefaultMaxRetries() {
            return defaultMaxRetries;
        }

        public void setDefaultMaxRetries(int defaultMaxRetries) {
            this.defaultMaxRetries = defaultMaxRetries;
        }

        public Duration getSchedulerInterval() {
            return schedulerInterval;
        }

        public void setSchedulerInterval(Duration schedulerInterval) {
            this.schedulerInterval = schedulerInterval;
        }

        public int getBatchSize() {
            return batchSize;
        }

        public void setBatchSize(int batchSize) {
            this.batchSize = batchSize;
        }

        public boolean isEnabled() {
            return enabled;
        }

        public void setEnabled(boolean enabled) {
            this.enabled = enabled;
        }
    }

    public static class Outbox {

        private boolean enabled = true;
        private Duration pollInterval = Duration.ofMillis(500);
        private int batchSize = 100;

        /** How long to wait for a Kafka broker acknowledgement before treating the send as failed. */
        private Duration publishTimeout = Duration.ofSeconds(10);

        public boolean isEnabled() {
            return enabled;
        }

        public void setEnabled(boolean enabled) {
            this.enabled = enabled;
        }

        public Duration getPollInterval() {
            return pollInterval;
        }

        public void setPollInterval(Duration pollInterval) {
            this.pollInterval = pollInterval;
        }

        public int getBatchSize() {
            return batchSize;
        }

        public void setBatchSize(int batchSize) {
            this.batchSize = batchSize;
        }

        public Duration getPublishTimeout() {
            return publishTimeout;
        }

        public void setPublishTimeout(Duration publishTimeout) {
            this.publishTimeout = publishTimeout;
        }
    }

    public static class Recovery {

        private boolean enabled = true;
        private Duration interval = Duration.ofSeconds(10);
        private int batchSize = 50;

        /**
         * Extra grace granted to a job whose lease expired but whose worker is still
         * heartbeating — most likely a slow renewal rather than a dead process.
         */
        private Duration liveWorkerGrace = Duration.ofSeconds(30);

        public boolean isEnabled() {
            return enabled;
        }

        public void setEnabled(boolean enabled) {
            this.enabled = enabled;
        }

        public Duration getInterval() {
            return interval;
        }

        public void setInterval(Duration interval) {
            this.interval = interval;
        }

        public int getBatchSize() {
            return batchSize;
        }

        public void setBatchSize(int batchSize) {
            this.batchSize = batchSize;
        }

        public Duration getLiveWorkerGrace() {
            return liveWorkerGrace;
        }

        public void setLiveWorkerGrace(Duration liveWorkerGrace) {
            this.liveWorkerGrace = liveWorkerGrace;
        }
    }

    public static class Cache {

        private boolean enabled = true;
        private Duration jobTtl = Duration.ofSeconds(120);

        public boolean isEnabled() {
            return enabled;
        }

        public void setEnabled(boolean enabled) {
            this.enabled = enabled;
        }

        public Duration getJobTtl() {
            return jobTtl;
        }

        public void setJobTtl(Duration jobTtl) {
            this.jobTtl = jobTtl;
        }
    }

    public static class Topics {

        private String jobs = "taskforge.jobs";
        private String retry = "taskforge.jobs.retry";
        private String dlq = "taskforge.jobs.dlq";
        private int partitions = 6;
        private short replicationFactor = 1;

        public String getJobs() {
            return jobs;
        }

        public void setJobs(String jobs) {
            this.jobs = jobs;
        }

        public String getRetry() {
            return retry;
        }

        public void setRetry(String retry) {
            this.retry = retry;
        }

        public String getDlq() {
            return dlq;
        }

        public void setDlq(String dlq) {
            this.dlq = dlq;
        }

        public int getPartitions() {
            return partitions;
        }

        public void setPartitions(int partitions) {
            this.partitions = partitions;
        }

        public short getReplicationFactor() {
            return replicationFactor;
        }

        public void setReplicationFactor(short replicationFactor) {
            this.replicationFactor = replicationFactor;
        }
    }

    public static class Limits {

        /** Serialized job payload ceiling, enforced before anything is persisted. */
        private int maxPayloadBytes = 8192;
        private int maxRetries = 10;
        private int maxTimeoutSeconds = 3600;
        private int defaultTimeoutSeconds = 60;

        public int getMaxPayloadBytes() {
            return maxPayloadBytes;
        }

        public void setMaxPayloadBytes(int maxPayloadBytes) {
            this.maxPayloadBytes = maxPayloadBytes;
        }

        public int getMaxRetries() {
            return maxRetries;
        }

        public void setMaxRetries(int maxRetries) {
            this.maxRetries = maxRetries;
        }

        public int getMaxTimeoutSeconds() {
            return maxTimeoutSeconds;
        }

        public void setMaxTimeoutSeconds(int maxTimeoutSeconds) {
            this.maxTimeoutSeconds = maxTimeoutSeconds;
        }

        public int getDefaultTimeoutSeconds() {
            return defaultTimeoutSeconds;
        }

        public void setDefaultTimeoutSeconds(int defaultTimeoutSeconds) {
            this.defaultTimeoutSeconds = defaultTimeoutSeconds;
        }
    }
}
