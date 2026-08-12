package com.taskforge.service;

import com.taskforge.exception.InvalidStateTransitionException;
import com.taskforge.model.JobStatus;
import org.springframework.stereotype.Component;

import java.util.EnumMap;
import java.util.EnumSet;
import java.util.Map;
import java.util.Set;

import static com.taskforge.model.JobStatus.CANCELLED;
import static com.taskforge.model.JobStatus.CANCEL_REQUESTED;
import static com.taskforge.model.JobStatus.COMPLETED;
import static com.taskforge.model.JobStatus.DEAD_LETTERED;
import static com.taskforge.model.JobStatus.FAILED;
import static com.taskforge.model.JobStatus.QUEUED;
import static com.taskforge.model.JobStatus.RETRYING;
import static com.taskforge.model.JobStatus.RUNNING;
import static com.taskforge.model.JobStatus.TIMED_OUT;

/**
 * The single authority on which job state transitions are legal.
 *
 * <p>Nothing else in the codebase is allowed to decide that a status change is acceptable.
 * The SQL statements in {@link com.taskforge.repository.JobRepository} enforce these same
 * edges in their {@code WHERE} clauses; this class is what makes the rules readable,
 * testable, and impossible to drift apart from silently.
 */
@Component
public class JobStateMachine {

    private static final Map<JobStatus, Set<JobStatus>> ALLOWED = buildTransitionTable();

    private static Map<JobStatus, Set<JobStatus>> buildTransitionTable() {
        Map<JobStatus, Set<JobStatus>> table = new EnumMap<>(JobStatus.class);

        // Waiting for a worker. Can be claimed, or cancelled outright before it ever runs.
        table.put(QUEUED, EnumSet.of(RUNNING, CANCELLED));

        // Owned by a worker. Every way an attempt can end lives here.
        table.put(RUNNING, EnumSet.of(
                COMPLETED,          // handler returned success
                RETRYING,           // retryable failure, budget remaining
                DEAD_LETTERED,      // retryable failure, budget exhausted
                FAILED,             // handler declared the failure permanent
                CANCEL_REQUESTED,   // a client asked it to stop
                TIMED_OUT));        // exceeded its deadline, or its lease lapsed

        // Backoff window. The retry scheduler re-queues it; a client may still cancel it.
        table.put(RETRYING, EnumSet.of(QUEUED, DEAD_LETTERED, CANCELLED));

        // Cancellation is cooperative, so the job may still finish, fail or time out first.
        table.put(CANCEL_REQUESTED, EnumSet.of(CANCELLED, COMPLETED, FAILED, TIMED_OUT, DEAD_LETTERED, RETRYING));

        // Recovery decides whether a timed-out attempt is worth repeating.
        table.put(TIMED_OUT, EnumSet.of(RETRYING, DEAD_LETTERED));

        // Terminal states have no outgoing edges at all.
        table.put(COMPLETED, EnumSet.noneOf(JobStatus.class));
        table.put(FAILED, EnumSet.noneOf(JobStatus.class));
        table.put(CANCELLED, EnumSet.noneOf(JobStatus.class));
        table.put(DEAD_LETTERED, EnumSet.noneOf(JobStatus.class));

        return table;
    }

    public boolean isValidTransition(JobStatus from, JobStatus to) {
        if (from == null || to == null) {
            return false;
        }
        return ALLOWED.getOrDefault(from, EnumSet.noneOf(JobStatus.class)).contains(to);
    }

    /** @throws InvalidStateTransitionException if the edge is not part of the lifecycle. */
    public void validateTransition(JobStatus from, JobStatus to) {
        if (!isValidTransition(from, to)) {
            throw new InvalidStateTransitionException(from, to);
        }
    }

    public Set<JobStatus> allowedTransitionsFrom(JobStatus from) {
        return EnumSet.copyOf(ALLOWED.getOrDefault(from, EnumSet.noneOf(JobStatus.class)));
    }

    /** True when a client may still meaningfully ask for this job to be cancelled. */
    public boolean isCancellable(JobStatus status) {
        return isValidTransition(status, CANCELLED) || isValidTransition(status, CANCEL_REQUESTED);
    }
}
