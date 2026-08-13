package com.taskforge.service;

import com.taskforge.exception.InvalidStateTransitionException;
import com.taskforge.model.JobStatus;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.EnumSource;

import static com.taskforge.model.JobStatus.CANCELLED;
import static com.taskforge.model.JobStatus.CANCEL_REQUESTED;
import static com.taskforge.model.JobStatus.COMPLETED;
import static com.taskforge.model.JobStatus.DEAD_LETTERED;
import static com.taskforge.model.JobStatus.FAILED;
import static com.taskforge.model.JobStatus.QUEUED;
import static com.taskforge.model.JobStatus.RETRYING;
import static com.taskforge.model.JobStatus.RUNNING;
import static com.taskforge.model.JobStatus.TIMED_OUT;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class JobStateMachineTest {

    private final JobStateMachine stateMachine = new JobStateMachine();

    @ParameterizedTest(name = "{0} -> {1} is allowed")
    @CsvSource({
            "QUEUED, RUNNING",
            "QUEUED, CANCELLED",
            "RUNNING, COMPLETED",
            "RUNNING, RETRYING",
            "RUNNING, DEAD_LETTERED",
            "RUNNING, FAILED",
            "RUNNING, CANCEL_REQUESTED",
            "RUNNING, TIMED_OUT",
            "RETRYING, QUEUED",
            "RETRYING, DEAD_LETTERED",
            "RETRYING, CANCELLED",
            "CANCEL_REQUESTED, CANCELLED",
            "CANCEL_REQUESTED, COMPLETED",
            "TIMED_OUT, RETRYING",
            "TIMED_OUT, DEAD_LETTERED"
    })
    void allowsLifecycleTransitions(JobStatus from, JobStatus to) {
        assertThat(stateMachine.isValidTransition(from, to)).isTrue();
    }

    @ParameterizedTest(name = "{0} -> {1} is rejected")
    @CsvSource({
            // Work never flows backwards.
            "RUNNING, QUEUED",
            "COMPLETED, RUNNING",
            "COMPLETED, RETRYING",
            "CANCELLED, QUEUED",
            "CANCELLED, RUNNING",
            "DEAD_LETTERED, RUNNING",
            "DEAD_LETTERED, QUEUED",
            "FAILED, RETRYING",
            // A job cannot finish without having run.
            "QUEUED, COMPLETED",
            "QUEUED, RETRYING",
            "QUEUED, TIMED_OUT",
            // A job waiting on backoff is not executing.
            "RETRYING, RUNNING",
            "RETRYING, COMPLETED",
            // Timed out is resolved by recovery, not by finishing.
            "TIMED_OUT, COMPLETED",
            "TIMED_OUT, RUNNING"
    })
    void rejectsIllegalTransitions(JobStatus from, JobStatus to) {
        assertThat(stateMachine.isValidTransition(from, to)).isFalse();
    }

    @ParameterizedTest
    @EnumSource(value = JobStatus.class, names = {"COMPLETED", "FAILED", "CANCELLED", "DEAD_LETTERED"})
    @DisplayName("terminal states have no outgoing transitions at all")
    void terminalStatesAreDeadEnds(JobStatus terminal) {
        assertThat(terminal.isTerminal()).isTrue();
        assertThat(stateMachine.allowedTransitionsFrom(terminal)).isEmpty();

        for (JobStatus target : JobStatus.values()) {
            assertThat(stateMachine.isValidTransition(terminal, target))
                    .as("%s -> %s must be rejected", terminal, target)
                    .isFalse();
        }
    }

    @ParameterizedTest
    @EnumSource(value = JobStatus.class, names = {"QUEUED", "RUNNING", "RETRYING", "CANCEL_REQUESTED", "TIMED_OUT"})
    void nonTerminalStatesAreNotTerminal(JobStatus status) {
        assertThat(status.isTerminal()).isFalse();
    }

    @Test
    void validateThrowsOnIllegalTransition() {
        assertThatThrownBy(() -> stateMachine.validateTransition(COMPLETED, RUNNING))
                .isInstanceOf(InvalidStateTransitionException.class)
                .hasMessageContaining("COMPLETED")
                .hasMessageContaining("RUNNING");
    }

    @Test
    void validatePassesOnLegalTransition() {
        stateMachine.validateTransition(QUEUED, RUNNING);
    }

    @Test
    void nullsAreNeverValid() {
        assertThat(stateMachine.isValidTransition(null, RUNNING)).isFalse();
        assertThat(stateMachine.isValidTransition(QUEUED, null)).isFalse();
        assertThat(stateMachine.isValidTransition(null, null)).isFalse();
    }

    @Test
    @DisplayName("a job may be cancelled up until it reaches a terminal state")
    void cancellableStates() {
        assertThat(stateMachine.isCancellable(QUEUED)).isTrue();
        assertThat(stateMachine.isCancellable(RUNNING)).isTrue();
        assertThat(stateMachine.isCancellable(RETRYING)).isTrue();
        assertThat(stateMachine.isCancellable(CANCEL_REQUESTED)).isTrue();

        assertThat(stateMachine.isCancellable(COMPLETED)).isFalse();
        assertThat(stateMachine.isCancellable(DEAD_LETTERED)).isFalse();
        assertThat(stateMachine.isCancellable(CANCELLED)).isFalse();
        assertThat(stateMachine.isCancellable(FAILED)).isFalse();
    }

    @Test
    @DisplayName("a cancellation request can still lose the race to a job that finishes first")
    void cancelRequestedMayStillComplete() {
        assertThat(stateMachine.isValidTransition(CANCEL_REQUESTED, COMPLETED)).isTrue();
        assertThat(stateMachine.isValidTransition(CANCEL_REQUESTED, TIMED_OUT)).isTrue();
    }
}
