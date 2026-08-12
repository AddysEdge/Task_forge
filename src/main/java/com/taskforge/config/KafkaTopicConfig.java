package com.taskforge.config;

import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.TopicBuilder;

/**
 * Declares the three topics so a fresh environment works without manual broker setup.
 *
 * <p>Partition count is what actually spreads work across worker processes: consumers in
 * the {@code taskforge-workers} group are assigned disjoint partitions, so six partitions
 * across two workers means each owns three. The job id is the partition key, which keeps all
 * events for one job in order relative to each other.
 */
@Configuration
public class KafkaTopicConfig {

    private final TaskForgeProperties properties;

    public KafkaTopicConfig(TaskForgeProperties properties) {
        this.properties = properties;
    }

    @Bean
    public NewTopic jobsTopic() {
        return topic(properties.getTopics().getJobs());
    }

    @Bean
    public NewTopic jobsRetryTopic() {
        return topic(properties.getTopics().getRetry());
    }

    /** Terminal by design: workers never subscribe to this one. */
    @Bean
    public NewTopic jobsDlqTopic() {
        return topic(properties.getTopics().getDlq());
    }

    private NewTopic topic(String name) {
        return TopicBuilder.name(name)
                .partitions(properties.getTopics().getPartitions())
                .replicas(properties.getTopics().getReplicationFactor())
                .build();
    }
}
