package com.taskforge.config;

import jakarta.annotation.PreDestroy;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.DescribeClusterOptions;
import org.apache.kafka.clients.admin.DescribeClusterResult;
import org.springframework.boot.actuate.health.Health;
import org.springframework.boot.actuate.health.HealthIndicator;
import org.springframework.kafka.core.KafkaAdmin;
import org.springframework.stereotype.Component;

import java.util.concurrent.TimeUnit;

/**
 * Reports Kafka reachability under {@code /actuator/health}.
 *
 * <p>Spring Boot ships health indicators for PostgreSQL and Redis but not for Kafka, so this
 * fills the gap: without it a broker outage would leave the service reporting healthy while
 * no job could be dispatched.
 *
 * <p>The admin client is created once and reused; building one per health check would open a
 * fresh set of broker connections every few seconds.
 */
@Component
public class KafkaHealthIndicator implements HealthIndicator {

    private static final int DESCRIBE_TIMEOUT_MS = 2000;

    private final AdminClient adminClient;

    public KafkaHealthIndicator(KafkaAdmin kafkaAdmin) {
        this.adminClient = AdminClient.create(kafkaAdmin.getConfigurationProperties());
    }

    @Override
    public Health health() {
        try {
            DescribeClusterResult cluster = adminClient.describeCluster(
                    new DescribeClusterOptions().timeoutMs(DESCRIBE_TIMEOUT_MS));

            int nodeCount = cluster.nodes().get(DESCRIBE_TIMEOUT_MS, TimeUnit.MILLISECONDS).size();
            String clusterId = cluster.clusterId().get(DESCRIBE_TIMEOUT_MS, TimeUnit.MILLISECONDS);

            return Health.up()
                    .withDetail("clusterId", clusterId)
                    .withDetail("nodes", nodeCount)
                    .build();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return Health.down().withDetail("error", "Interrupted while querying the Kafka cluster").build();
        } catch (Exception e) {
            return Health.down().withDetail("error", e.getMessage()).build();
        }
    }

    @PreDestroy
    public void close() {
        adminClient.close();
    }
}
