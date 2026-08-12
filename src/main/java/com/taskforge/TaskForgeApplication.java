package com.taskforge;

import com.taskforge.config.TaskForgeProperties;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.scheduling.annotation.EnableScheduling;

/**
 * Single codebase, two roles. {@code APP_ROLE} selects the active Spring profile:
 * {@code api} serves REST traffic, {@code worker} consumes Kafka and executes jobs, and
 * {@code all} (the default, used for local runs and tests) does both in one process.
 */
@SpringBootApplication
@EnableScheduling
@EnableConfigurationProperties(TaskForgeProperties.class)
public class TaskForgeApplication {

    public static void main(String[] args) {
        SpringApplication.run(TaskForgeApplication.class, args);
    }
}
