package com.taskforge.retry;

import com.taskforge.config.TaskForgeProperties;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.time.Duration;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class BackoffCalculatorTest {

    private TaskForgeProperties properties;
    private BackoffCalculator calculator;

    @BeforeEach
    void setUp() {
        properties = new TaskForgeProperties();
        properties.getRetry().setInitialDelay(Duration.ofSeconds(1));
        properties.getRetry().setMultiplier(2.0);
        properties.getRetry().setMaxDelay(Duration.ofSeconds(60));
        calculator = new BackoffCalculator(properties);
    }

    @Test
    @DisplayName("delays double with each retry: 1s, 2s, 4s, 8s")
    void doublesEachRetry() {
        assertThat(calculator.delayForRetry(1)).isEqualTo(Duration.ofSeconds(1));
        assertThat(calculator.delayForRetry(2)).isEqualTo(Duration.ofSeconds(2));
        assertThat(calculator.delayForRetry(3)).isEqualTo(Duration.ofSeconds(4));
        assertThat(calculator.delayForRetry(4)).isEqualTo(Duration.ofSeconds(8));
    }

    @Test
    void capsAtMaxDelay() {
        assertThat(calculator.delayForRetry(20)).isEqualTo(Duration.ofSeconds(60));
        assertThat(calculator.delayForRetry(100)).isEqualTo(Duration.ofSeconds(60));
    }

    @Test
    @DisplayName("a huge retry number cannot overflow into a negative or absurd delay")
    void extremeRetryNumbersStayBounded() {
        Duration delay = calculator.delayForRetry(Integer.MAX_VALUE);
        assertThat(delay).isBetween(Duration.ZERO, Duration.ofSeconds(60));
    }

    @Test
    void honoursCustomConfiguration() {
        properties.getRetry().setInitialDelay(Duration.ofMillis(250));
        properties.getRetry().setMultiplier(3.0);
        properties.getRetry().setMaxDelay(Duration.ofSeconds(5));

        assertThat(calculator.delayForRetry(1)).isEqualTo(Duration.ofMillis(250));
        assertThat(calculator.delayForRetry(2)).isEqualTo(Duration.ofMillis(750));
        assertThat(calculator.delayForRetry(3)).isEqualTo(Duration.ofMillis(2250));
        assertThat(calculator.delayForRetry(4)).isEqualTo(Duration.ofSeconds(5));
    }

    @Test
    @DisplayName("a multiplier of 1 gives fixed-interval retries")
    void multiplierOfOneIsConstant() {
        properties.getRetry().setMultiplier(1.0);

        assertThat(calculator.delayForRetry(1)).isEqualTo(Duration.ofSeconds(1));
        assertThat(calculator.delayForRetry(5)).isEqualTo(Duration.ofSeconds(1));
    }

    @Test
    void rejectsRetryNumbersBelowOne() {
        assertThatThrownBy(() -> calculator.delayForRetry(0))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> calculator.delayForRetry(-3))
                .isInstanceOf(IllegalArgumentException.class);
    }
}
