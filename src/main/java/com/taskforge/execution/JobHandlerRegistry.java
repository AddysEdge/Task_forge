package com.taskforge.execution;

import com.taskforge.model.JobType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import java.util.EnumMap;
import java.util.List;
import java.util.Map;

/**
 * Resolves a {@link JobType} to its handler.
 *
 * <p>Both failure modes are detected at startup rather than at execution time: two handlers
 * claiming the same type, and a type with no handler at all. A misconfigured deployment
 * therefore refuses to boot instead of dead-lettering every job of one type at runtime.
 */
@Component
public class JobHandlerRegistry {

    private static final Logger log = LoggerFactory.getLogger(JobHandlerRegistry.class);

    private final Map<JobType, JobHandler> handlers = new EnumMap<>(JobType.class);

    public JobHandlerRegistry(List<JobHandler> discoveredHandlers) {
        for (JobHandler handler : discoveredHandlers) {
            JobHandler previous = handlers.put(handler.supportedType(), handler);
            if (previous != null) {
                throw new IllegalStateException(
                        "Job type %s is claimed by both %s and %s".formatted(
                                handler.supportedType(),
                                previous.getClass().getName(),
                                handler.getClass().getName()));
            }
        }

        for (JobType type : JobType.values()) {
            if (!handlers.containsKey(type)) {
                throw new IllegalStateException("No JobHandler registered for job type " + type);
            }
        }

        log.info("Registered {} job handlers: {}", handlers.size(), handlers.keySet());
    }

    public JobHandler handlerFor(JobType type) {
        JobHandler handler = handlers.get(type);
        if (handler == null) {
            throw new IllegalStateException("No JobHandler registered for job type " + type);
        }
        return handler;
    }
}
