package com.taskforge.it;

import com.taskforge.dto.JobResponse;
import com.taskforge.entity.JobEntity;
import com.taskforge.model.JobStatus;
import com.taskforge.service.JobLifecycleService;
import com.taskforge.support.AbstractIntegrationTest;
import com.taskforge.support.JobFixtures;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.redis.core.StringRedisTemplate;

import java.time.Duration;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * The cache must never be able to contradict PostgreSQL.
 *
 * <p>Jobs here are inserted directly rather than submitted, so no worker is dispatched and
 * the only state changes are the ones each test makes deliberately.
 */
class CacheConsistencyIT extends AbstractIntegrationTest {

    @Autowired
    private StringRedisTemplate redis;

    @Autowired
    private JobLifecycleService jobLifecycleService;

    private String cacheKey(JobEntity job) {
        return "job:" + job.getId();
    }

    @Test
    @DisplayName("reading a job populates the cache, and a later state change removes it")
    void stateChangesInvalidateTheCachedJob() {
        JobEntity job = jobRepository.saveAndFlush(JobFixtures.queuedDelay());
        redis.delete(cacheKey(job));

        JobResponse firstRead = rest.getForObject("/api/v1/jobs/{id}", JobResponse.class, job.getId());
        assertThat(firstRead.status()).isEqualTo(JobStatus.QUEUED);
        assertThat(redis.hasKey(cacheKey(job)))
                .as("the read should have populated the cache")
                .isTrue();

        // Authoritative state changes underneath the cache.
        assertThat(jobLifecycleService.claim(job.getId(), "cache-test-worker")).isPresent();

        Awaitility.await("cache entry to be invalidated after commit")
                .atMost(Duration.ofSeconds(10))
                .pollInterval(Duration.ofMillis(100))
                .untilAsserted(() -> assertThat(redis.hasKey(cacheKey(job))).isFalse());
    }

    @Test
    @DisplayName("after a state change the API serves the new state, not the cached one")
    void apiReadsReflectTheNewStateAfterATransition() {
        JobEntity job = jobRepository.saveAndFlush(JobFixtures.queuedDelay());

        // Prime the cache with QUEUED.
        assertThat(rest.getForObject("/api/v1/jobs/{id}", JobResponse.class, job.getId()).status())
                .isEqualTo(JobStatus.QUEUED);

        jobLifecycleService.claim(job.getId(), "cache-test-worker-2");

        Awaitility.await("API to serve the post-transition state")
                .atMost(Duration.ofSeconds(10))
                .pollInterval(Duration.ofMillis(100))
                .untilAsserted(() -> {
                    JobResponse fresh = rest.getForObject(
                            "/api/v1/jobs/{id}", JobResponse.class, job.getId());
                    assertThat(fresh.status()).isEqualTo(JobStatus.RUNNING);
                    assertThat(fresh.workerId()).isEqualTo("cache-test-worker-2");
                });
    }

    @Test
    @DisplayName("completion invalidates the cache too, so results are never hidden by a stale entry")
    void completionIsVisibleImmediatelyAfterCaching() throws Exception {
        JobEntity job = jobRepository.saveAndFlush(JobFixtures.queuedDelay());
        jobLifecycleService.claim(job.getId(), "cache-test-worker-3");

        // Cache the RUNNING view.
        assertThat(rest.getForObject("/api/v1/jobs/{id}", JobResponse.class, job.getId()).status())
                .isEqualTo(JobStatus.RUNNING);

        jobLifecycleService.markCompleted(
                job.getId(), "cache-test-worker-3", objectMapper.readTree("{\"sleptMs\":50}"), 50);

        Awaitility.await("completed state to be served")
                .atMost(Duration.ofSeconds(10))
                .pollInterval(Duration.ofMillis(100))
                .untilAsserted(() -> {
                    JobResponse fresh = rest.getForObject(
                            "/api/v1/jobs/{id}", JobResponse.class, job.getId());
                    assertThat(fresh.status()).isEqualTo(JobStatus.COMPLETED);
                    assertThat(fresh.result()).isNotNull();
                    assertThat(fresh.result().get("sleptMs").asInt()).isEqualTo(50);
                });
    }

    @Test
    @DisplayName("PostgreSQL wins: a cache entry that disagrees is discarded, not trusted")
    void databaseRemainsAuthoritativeOverACorruptCacheEntry() {
        JobEntity job = jobRepository.saveAndFlush(JobFixtures.queuedDelay());

        // Simulate a cache entry that can no longer be read (e.g. an older serialisation).
        redis.opsForValue().set(cacheKey(job), "{not-valid-json", Duration.ofMinutes(1));

        JobResponse response = rest.getForObject("/api/v1/jobs/{id}", JobResponse.class, job.getId());

        assertThat(response.status()).isEqualTo(JobStatus.QUEUED);
        assertThat(response.id()).isEqualTo(job.getId());
    }

    @Test
    @DisplayName("a cache miss is served from PostgreSQL and repopulates Redis")
    void cacheMissFallsBackToTheDatabase() {
        JobEntity job = jobRepository.saveAndFlush(JobFixtures.queuedDelay());
        redis.delete(cacheKey(job));

        JobResponse response = rest.getForObject("/api/v1/jobs/{id}", JobResponse.class, job.getId());

        assertThat(response.id()).isEqualTo(job.getId());
        assertThat(redis.hasKey(cacheKey(job))).isTrue();
    }
}
