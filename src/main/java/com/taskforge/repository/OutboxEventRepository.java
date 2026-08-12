package com.taskforge.repository;

import com.taskforge.entity.OutboxEventEntity;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Modifying;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;
import org.springframework.stereotype.Repository;

import java.time.Instant;
import java.util.List;
import java.util.UUID;

@Repository
public interface OutboxEventRepository extends JpaRepository<OutboxEventEntity, UUID> {

    /**
     * Claims a batch of unpublished events for this publisher instance.
     *
     * <p>{@code FOR UPDATE SKIP LOCKED} is what makes it safe to run the publisher on the API
     * process and on every worker at the same time: each row is delivered to exactly one
     * scanner, and a slow publisher blocks nobody.
     */
    @Query(value = """
            SELECT *
              FROM outbox_events
             WHERE status = 'PENDING'
             ORDER BY created_at
             LIMIT :limit
               FOR UPDATE SKIP LOCKED
            """, nativeQuery = true)
    List<OutboxEventEntity> lockPendingBatch(@Param("limit") int limit);

    @Modifying(clearAutomatically = true, flushAutomatically = true)
    @Query(value = """
            UPDATE outbox_events
               SET status = 'PUBLISHED',
                   published_at = :now,
                   attempt_count = attempt_count + 1,
                   last_error = NULL
             WHERE id = :id
               AND status = 'PENDING'
            """, nativeQuery = true)
    int markPublished(@Param("id") UUID id, @Param("now") Instant now);

    /** Records a failed publication attempt while leaving the event PENDING for the next pass. */
    @Modifying(clearAutomatically = true, flushAutomatically = true)
    @Query(value = """
            UPDATE outbox_events
               SET attempt_count = attempt_count + 1,
                   last_error = :error
             WHERE id = :id
            """, nativeQuery = true)
    int recordPublishFailure(@Param("id") UUID id, @Param("error") String error);

    long countByStatus(OutboxEventEntity.Status status);

    List<OutboxEventEntity> findByAggregateIdOrderByCreatedAt(UUID aggregateId);
}
