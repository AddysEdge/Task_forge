package com.taskforge.config;

import com.fasterxml.jackson.databind.DeserializationFeature;
import org.springframework.boot.autoconfigure.jackson.Jackson2ObjectMapperBuilderCustomizer;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class JacksonConfig {

    /**
     * Rejects unknown properties on request bodies so a typo in a field name is a 400 rather
     * than a silently ignored setting — a caller who writes {@code maxRetry} instead of
     * {@code maxRetries} should be told, not quietly given the default.
     *
     * <p>Wire formats that must tolerate version skew opt out individually; see
     * {@link com.taskforge.kafka.JobEvent}.
     */
    @Bean
    public Jackson2ObjectMapperBuilderCustomizer strictDeserialization() {
        return builder -> builder.featuresToEnable(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES);
    }
}
