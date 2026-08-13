# TaskForge — Handoff Report

**Status:** Dismantled / archived
**Date:** 2026-08-12
**Final build:** `./mvnw clean verify` — BUILD SUCCESS, 113 unit + 47 integration tests passing
**Runtime state:** all containers, volumes and networks destroyed; no processes remain

This document is written for someone who was not involved in building TaskForge and may need to
resume, evaluate, or salvage it. It records what genuinely works, what does not, and what was left
unfinished — including the parts that are weaker than the README implies.

---

## 1. What this project is

A distributed job execution and scheduling platform. Clients submit jobs over a REST API; jobs are
announced to Kafka through a transactional outbox and executed concurrently by independent worker
processes. PostgreSQL is the single source of truth; Redis is an expendable cache and liveness store.

The whole design follows from one assumption: **any message can be delivered twice, and any process
can die at any moment.**

Java 21 · Spring Boot 3.3.5 · PostgreSQL 16 · Kafka 3.8 (KRaft) · Redis 7 · Flyway · Docker Compose ·
Testcontainers · GitHub Actions.

Scale: 63 main source files, 18 test files, 2 migrations.

---

## 2. Verified working

Everything below was demonstrated against the real Docker Compose environment, not inferred.

| Capability | Evidence observed |
|---|---|
| Full lifecycle | `QUEUED → RUNNING → COMPLETED`, result `{"sleptMs":4000}` |
| Real multi-process distribution | 10 COMPUTE jobs split **3 on worker-1, 7 on worker-2** (separate containers) |
| Idempotency under race | 16 simultaneous POSTs with one key → exactly one `201`, one DB row |
| Atomic claim | 8 threads racing one job → exactly one winner |
| Retry + exponential backoff | `attempt:3, retryCount:2`, timing consistent with 1s/2s/4s |
| Dead-lettering | Outbox trail `JOB_SUBMITTED → 2× JOB_RETRY_SCHEDULED → JOB_DEAD_LETTERED`, all PUBLISHED |
| Duplicate Kafka delivery | Re-published event for a completed job → row version unchanged; log `not claimable (status=COMPLETED)` |
| Cooperative cancellation | 120-second sleep cancelled in ~1.8 s |
| Pre-execution cancellation | Job cancelled with `startedAt` never set |
| Timeout | 60 s job with 3 s timeout → `DEAD_LETTERED`, "exceeded its timeout of 3 seconds" |
| Crashed-worker recovery | Job owned by a non-existent `ghost-worker-42` with expired lease → recovered → retried → completed on `worker-2` |
| Heartbeat expiry | Redis TTL observed at 28 s; missing key drives immediate reclaim |
| Cache invalidation | Verified directly against Redis keys before/after transition |
| Outbox under failure | Real unpublishable topic against the real producer, then repaired → published |
| **Restart durability** | All three app containers restarted mid-retry; `retryCount` continued 2 → 4 → 5 → 6, completed on attempt 7 with no reset |
| Stability | Infra containers healthy for 2 h, app containers 41 min post-restart, no crashes |

**Final data state at teardown:** 33 jobs, all in terminal states (29 COMPLETED, 2 CANCELLED,
2 DEAD_LETTERED). **45 of 45 outbox events PUBLISHED** — zero stranded events.

---

## 3. Known gaps — read this before trusting the README

These are real and were found by auditing the code, not by testing.

### 🔴 HIGH — The state machine is dead code

`JobStateMachine` (`src/main/java/com/taskforge/service/JobStateMachine.java`) is documented, and has
44 passing tests. **It is referenced by zero lines of production code** — only tests and one javadoc
comment.

Actual enforcement lives in the SQL `WHERE` clauses in `JobRepository`. That works and is genuinely
concurrency-safe. But it means there are **two independent definitions of the lifecycle**: the Java
table (tested, unused) and the SQL guards (used, never tested as a table). They agree today only
because they were written to agree. Nothing prevents drift.

*Fix:* have `JobLifecycleService` consult it before each transition as a defensive assertion, or
delete it and document "the SQL is the state machine." **Effort: 1–2 h.**

### 🔴 HIGH — CI has never executed, and the workflow file is not in this repository

A GitHub Actions workflow was written at `.github/workflows/ci.yml`, but it is **absent from this
repository**. The publishing account's token carried `repo` scope but not `workflow`, so GitHub
rejected the push of that path, and the project was archived rather than pausing to re-authorise.
The workflow has therefore never run and is untested YAML.

**To add it**, from a clone:

```bash
gh auth refresh -h github.com -s workflow    # one-time browser authorisation
# recreate .github/workflows/ci.yml (contents summarised below), then:
git add .github/workflows/ci.yml && git commit -m "Add CI workflow" && git push
```

Intended pipeline: on push and pull request — checkout, `actions/setup-java@v4` with JDK 21 and Maven
caching, `./mvnw clean compile`, `./mvnw test`, `./mvnw verify`, upload of surefire/failsafe reports
on failure, and a second job building the Docker image with Buildx.

Two known risks when it first runs:

- **`mvnw` executable bit.** A Linux runner fails with *permission denied* if it is not recorded.
  **Pre-emptively fixed** — `mvnw` is committed as mode `100755`, and `.gitattributes` forces LF on it
  so it cannot arrive with CRLF and fail as `bad interpreter: /bin/sh^M`.
- **Docker API version.** The runner's daemon versus the pinned `docker.api.version=1.44` (see §5).
  If integration tests fail with *"Could not find a valid Docker environment"*, that is the cause.

*Fix:* grant the scope, commit the workflow, watch the first run. **Effort: 20 min.**

### 🟡 MEDIUM — `@Version` optimistic locking is decorative

All state changes go through native SQL that increments `version` manually. The only JPA writes in
the codebase are **two inserts**. Hibernate's optimistic locking therefore never fires, and the
`OptimisticLockingFailureException` handler in `GlobalExceptionHandler` is **unreachable**.

The concurrency is correct — the conditional `UPDATE` does all the work, which is arguably the better
design. But do not describe this project as "using optimistic locking." It uses compare-and-swap.

### 🟡 MEDIUM — Dead configuration

`taskforge.limits.max-retries` and `taskforge.limits.max-timeout-seconds` are defined in
`application.yml`, documented, and **never read**. Real bounds are hardcoded in `@Min`/`@Max` on
`CreateJobRequest` (0–10, 1–3600). The values happen to match, so nothing is broken — but editing the
YAML does nothing. **Effort to fix: 1 h.**

### 🟡 MEDIUM — Untested paths

No integration coverage for:

- **Lease loss mid-execution** (`StopReason.LEASE_LOST`) — unit-tested only; no test steals a lease
  from an actively running job
- **Executor backpressure** — the blocking rejection handler in `WorkerJobExecutor` has never been
  exercised
- **Graceful shutdown with jobs actually RUNNING** — the restart test had jobs in `RETRYING`
- **Outbox rollback** — nothing proves a failed job insert leaves no orphan event

### 🟢 LOW — Dead methods

`JobStatus.isActive()` and `JobLifecycleService.isTerminal()` are never called anywhere.
`JobExecutionResult.isSuccess()`, `WorkerHeartbeatStore.find()` and
`JobStateMachine.allowedTransitionsFrom()` are used only by tests. **Effort: 30 min.**

### 🟢 LOW — Multi-process distribution is demonstrated, not automated

The "different worker IDs" test runs in one JVM with different ID strings. True multi-process
behaviour was only shown manually via Docker Compose. Automating it needs compose-in-test (~3–4 h).

### Not implemented (correctly disclaimed in the README)

Metrics, distributed tracing, structured JSON logging, Kubernetes manifests, autoscaling,
authentication/authorisation, job priorities, cron/scheduled jobs, job dependencies, per-tenant
quotas, admin UI, OpenAPI docs. No load testing was ever performed — behaviour under queue saturation
is unknown.

---

## 4. Definition-of-done scorecard

**24 of 26 verified.**

| # | Item | Status |
|---|---|---|
| 1–6 | Compose environment starts; Postgres, Redis, Kafka, API, 2 workers all healthy | ✅ |
| 7–9 | Job executes; multiple jobs concurrent; jobs on different workers | ✅ |
| 10–11 | Concurrent idempotency → one job; duplicate Kafka does not re-execute | ✅ |
| 12–14 | Backoff retry; retry counters survive restart; permanent failures dead-letter | ✅ |
| 15–16 | Queued and running jobs cancellable | ✅ |
| 17–19 | Expired leases recovered; stale workers detected; timeouts follow recovery path | ✅ |
| 20–22 | PostgreSQL authoritative; cache invalidates; Kafka failure does not strand jobs | ✅ |
| 23–24 | `./mvnw clean test` and `./mvnw clean package` pass | ✅ |
| 25 | **GitHub Actions runs the test suite** | ❌ never executed; workflow file not in repo (see §3) |
| 26 | README works from a clean checkout | ⚠️ unverifiable until cloned |

---

## 5. Environment prerequisites and gotchas

The machine this was built on had **no JDK installed**. Setup performed:

1. `winget install Microsoft.OpenJDK.21` → `C:\Program Files\Microsoft\jdk-21.0.12.8-hotspot`
2. `JAVA_HOME` set at **user scope** (persistent) — a machine-level change made outside the repo
3. Maven was **not** installed; the repo's `mvnw` wrapper downloads Maven 3.9.9 itself

**Docker Engine API version.** Testcontainers' bundled docker-java client negotiates an older Engine
API version than Docker 29+ accepts (that daemon requires ≥ 1.40 and rejects the default with an
opaque `400`). The build pins `docker.api.version=1.44` in `pom.xml`, which works on every daemon from
Docker 25.0 (Jan 2024) onward. On an older daemon:

```bash
./mvnw clean verify -Ddocker.api.version=1.41
```

This cost roughly an hour of diagnosis and is the single most likely thing to confuse a new
environment. Symptom: *"Could not find a valid Docker environment"* despite Docker plainly working.

---

## 6. Running it

Requires only Docker and Docker Compose — the image builds the code.

```bash
docker compose up --build          # API + 2 workers + Postgres + Redis + Kafka
docker compose ps                  # expect 6 healthy
curl -s http://localhost:8080/actuator/health
docker compose down -v             # full teardown
```

Ports: API 8080, worker-1 8081, worker-2 8082, Postgres 5432, Redis 6379.

Tests:

```bash
./mvnw clean test      # 113 unit tests, no Docker needed
./mvnw clean verify    # + 47 Testcontainers integration tests, then packages
```

Worked API examples are in `README.md`.

---

## 7. Where to look first

In the order that makes the system make sense:

1. `repository/JobRepository.java` — every atomic transition; the core concurrency argument
2. `worker/JobExecutionService.java` — claim-or-skip; why duplicates are harmless
3. `service/JobLifecycleService.java` — all durable state changes, retry-vs-dead-letter decision
4. `outbox/OutboxPublisher.java` + `service/JobCreator.java` — the outbox pattern end to end
5. `service/JobStateMachine.java` — the lifecycle in one place (**but see §3: not wired in**)
6. `recovery/JobRecoveryService.java` — leases vs heartbeats, and which is authoritative
7. `service/JobService.java` — the idempotency race and why it is deliberately not transactional
8. `cache/JobCache.java` — invalidate-after-commit
9. `retry/RetryRequeueService.java` — waiting without anything actually waiting
10. `src/test/java/com/taskforge/it/JobExecutionIT.java` — the 8-way claim race test

---

## 8. Design decisions worth preserving

- **Kafka carries no authority.** A `JobEvent` holds a job id and metadata — no payload, no trusted
  status. It is a hint that a row is worth looking at. Workers always re-read PostgreSQL.
- **The claim is a compare-and-swap.** `UPDATE ... WHERE id = ? AND status = 'QUEUED'`. The `WHERE` is
  the compare, the `SET` is the swap, PostgreSQL makes both atomic. Every repository method returns a
  row count; `0` means "someone else won" and is never treated as an error.
- **Backoff is a timestamp, not a sleep.** `next_attempt_at` is persisted, so no consumer thread ever
  blocks for a retry delay and the schedule survives every process restarting.
- **The claim happens inside the worker thread pool, not the Kafka listener.** A job's lease starts
  when execution actually starts, not when it is queued in memory.
- **Cache invalidation is deferred to after-commit**, so a concurrent reader cannot repopulate from a
  pre-commit row.
- **Unexpected exceptions are treated as transient.** Only an explicit permanent failure skips the
  retry budget, so an ordinary bug does not burn the budget of a job that might have succeeded.

---

## 9. If work resumes — prioritized

| # | Task | Effort |
|---|---|---|
| 1 | Push and confirm the CI workflow runs green | 15 min |
| 2 | Wire `JobStateMachine` into `JobLifecycleService` | 1–2 h |
| 3 | Remove or genuinely enforce the dead `limits` config | 1 h |
| 4 | Delete the five unused methods | 30 min |
| 5 | Integration test for lease loss mid-execution | 2 h |
| 6 | Settle and document the `@Version` story precisely | 30 min |
| 7 | Backpressure and graceful-shutdown tests | 2–3 h |
| 8 | Micrometer metrics + structured JSON logging | 4–6 h |

Items 1–4 total roughly three hours and would close every gap that is uncomfortable to defend.

---

## 10. Teardown record

Performed 2026-08-12:

- `docker compose down -v` — 6 containers removed, `taskforge_postgres-data` and
  `taskforge_kafka-data` volumes removed, `taskforge_default` network removed
- All demo and test data destroyed with those volumes (it was synthetic; nothing of value was lost)
- Ports 8080, 8081, 8082, 5432, 6379, 9092 confirmed free
- No residual Java or Maven processes (the one remaining `java` process is the VS Code Red Hat Java
  language server, unrelated to this project, and was left running)
- The `taskforge:local` Docker image was **not** removed; delete with `docker rmi taskforge:local`
- Docker Desktop itself was left running

Machine-level changes made during development that were **not** reverted: the JDK 21 installation and
the user-scope `JAVA_HOME` variable (see §5).
