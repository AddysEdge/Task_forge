package com.taskforge.kafka;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.taskforge.worker.JobExecutionService;
import com.taskforge.worker.WorkerJobExecutor;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.annotation.Profile;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

/**
 * Subscribes workers to the jobs and retry topics.
 *
 * <p>All workers share one consumer group, so Kafka assigns each partition to exactly one of
 * them — that is what spreads jobs across worker processes. It is a distribution mechanism
 * and nothing more: receiving a message confers no right to execute, which is decided later
 * by the atomic claim in PostgreSQL.
 *
 * <p>The listener thread does almost nothing itself. It parses the notification and hands it
 * to the worker pool, so a slow job never stalls the partition it arrived on beyond the
 * pool's own backpressure.
 */
@Component
@Profile({"worker", "all"})
public class JobEventConsumer {

    private static final Logger log = LoggerFactory.getLogger(JobEventConsumer.class);

    private final ObjectMapper objectMapper;
    private final WorkerJobExecutor workerJobExecutor;
    private final JobExecutionService jobExecutionService;

    public JobEventConsumer(ObjectMapper objectMapper,
                            WorkerJobExecutor workerJobExecutor,
                            JobExecutionService jobExecutionService) {
        this.objectMapper = objectMapper;
        this.workerJobExecutor = workerJobExecutor;
        this.jobExecutionService = jobExecutionService;
    }

    @KafkaListener(
            topics = {"${taskforge.topics.jobs}", "${taskforge.topics.retry}"},
            groupId = "${spring.kafka.consumer.group-id}")
    public void onJobEvent(ConsumerRecord<String, String> record) {
        JobEvent event;
        try {
            event = objectMapper.readValue(record.value(), JobEvent.class);
        } catch (Exception e) {
            // A message we cannot parse will never become parseable. Acknowledging it keeps
            // the partition moving; the offset and payload are logged so it can be inspected.
            log.error("Discarding unparseable message topic={} partition={} offset={}: {}",
                    record.topic(), record.partition(), record.offset(), e.getMessage());
            return;
        }

        if (event.jobId() == null) {
            log.error("Discarding job event without a jobId topic={} offset={}", record.topic(), record.offset());
            return;
        }

        log.debug("Received eventType={} jobId={} topic={} partition={} offset={}",
                event.eventType(), event.jobId(), record.topic(), record.partition(), record.offset());

        workerJobExecutor.submit(() -> jobExecutionService.executeJob(event.jobId()));
    }
}
