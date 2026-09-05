# Implementation Action Plan

**Project:** `OxMQ` (Distributed Redis Job & Workflow Engine for Java)  
**Status:** In Progress (Tracked in `PROGRESS_TRACKER.md`)  

---

## Phase 1: Project Scaffolding & Build Infrastructure

- [x] `[SETUP-001]` Initialize multi-module Maven project with root `pom.xml`.
- [x] `[SETUP-002]` Configure target Java 21 LTS with Virtual Threads support.
- [x] `[SETUP-003]` Set up module: `oxmq-core` (minimal dependencies: Lettuce, Jackson, SLF4J, Micrometer).
- [x] `[SETUP-004]` Set up module: `oxmq-spring-boot-starter` (Spring Boot 3.x auto-configuration).
- [x] `[SETUP-005]` Set up module: `oxmq-benchmarks` (JMH microbenchmarking suite).
- [x] `[SETUP-006]` Set up module: `oxmq-samples` (`oxmq-sample-basic` and `oxmq-sample-spring-boot`).
- [x] `[SETUP-007]` Configure license (Apache 2.0) and `.gitignore`.
- [ ] `[SETUP-008]` Set up GitHub Actions CI workflow (Build, Unit Tests, Testcontainers Integration Tests).

---

## Phase 2: Redis Transport & Lua Script Management

- [x] `[REDIS-001]` Implement `RedisConnectionManager` abstraction over Lettuce `RedisClient` and `RedisClusterClient`.
- [x] `[REDIS-002]` Add support for Redis connection pooling, Sentinel, SSL/TLS, and Cluster topologies.
- [x] `[REDIS-003]` Build `LuaScriptManager` to load, cache SHA1 digests, and execute BullMQ Lua scripts efficiently.
- [x] `[REDIS-004]` Port and test core BullMQ Lua scripts:
  - [x] `addJob.lua` (Enqueue job, handle deduplication and delay).
  - [x] `moveToActive.lua` (Atomic job acquisition with worker lock and token check).
  - [x] `moveToFinished.lua` (Atomic completion, result storage, and parent notification).
  - [x] `retryJob.lua` (Re-queue failed job with backoff timestamp).
  - [x] `extendLock.lua` (Heartbeat / lease renewal for long-running jobs).
  - [x] `cleanQueue.lua` (Purge completed/failed jobs older than TTL).
  - [x] `pauseQueue.lua` (Global queue pause / resume state).
  - [x] `rateLimit.lua` (Sliding window token bucket rate limiter).

---

## Phase 3: Core Domain Models & Serialization

- [x] `[MODEL-001]` Define `Job<T>` model with metadata (`id`, `name`, `data`, `opts`, `progress`, `attemptsMade`, `timestamp`, `processedOn`, `finishedOn`, `returnvalue`, `failedReason`).
- [x] `[MODEL-002]` Define `JobOptions` builder (`delay`, `attempts`, `backoff`, `removeOnComplete`, `removeOnFail`, `jobId`, `priority`).
- [x] `[MODEL-003]` Define `BackoffStrategy` interface with `FixedBackoff`, `ExponentialBackoff`, and `CustomBackoff` implementations.
- [x] `[MODEL-004]` Define `JobState` enum (`WAITING`, `ACTIVE`, `DELAYED`, `COMPLETED`, `FAILED`, `PAUSED`, `STALLED`).
- [x] `[MODEL-005]` Create `JobSerializer` interface with default `JacksonJobSerializer` (supporting Java `Records`, complex generic types, and Polymorphic types).
- [x] `[MODEL-006]` Add custom type resolution support for deserializing job payloads without explicit `.class` parameters.

---

## Phase 4: Job Producer Engine (`Queue<T>`)

- [x] `[PROD-001]` Implement `Queue<T>` interface for enqueueing single jobs (`queue.add(name, data, opts)`).
- [x] `[PROD-002]` Implement batch job enqueueing (`queue.addBulk(List<JobRequest<T>>)`).
- [x] `[PROD-003]` Implement delayed job scheduling with millisecond timestamp calculation.
- [x] `[PROD-004]` Implement job deduplication via custom `jobId` and debounce windows.
- [x] `[PROD-005]` Implement job retrieval methods (`queue.getJob(id)`, `queue.getJobs(states, start, end)`).
- [x] `[PROD-006]` Implement queue lifecycle methods (`pause()`, `resume()`, `isPaused()`, `count()`, `clean()`, `obliterate()`).
- [x] `[PROD-007]` Ensure job insertion matches BullMQ Redis key layout exactly.

---

## Phase 5: Worker & Virtual Thread Dispatcher Engine

- [x] `[WORK-001]` Create `Worker<T>` interface and `WorkerOptions` builder (`concurrency`, `lockDuration`, `maxStalledCount`, `useVirtualThreads`).
- [x] `[WORK-002]` Implement Java 21 `VirtualThreadPerTaskExecutor` dispatcher with automatic fallback to platform thread pools on Java 17.
- [x] `[WORK-003]` Build non-blocking job polling loop using Redis Streams / BRPOPLPUSH / `moveToActive.lua`.
- [x] `[WORK-004]` Implement type-safe `JobProcessor<T, R>` functional interface (`R process(Job<T> job) throws Exception`).
- [x] `[WORK-005]` Implement real-time progress update API (`job.updateProgress(int percentage)` and `job.updateProgress(Object payload)`).
- [x] `[WORK-006]` Implement job log appending API (`job.log(String message)` stored in Redis list).
- [x] `[WORK-007]` Implement graceful shutdown mechanism (`worker.close()` with configurable drain timeout).

---

## Phase 6: Resilience, Stalled Jobs & Heartbeats

- [x] `[RESL-001]` Implement `LockExtender` background task to renew Redis worker lock before `lockDuration` expires during long jobs.
- [x] `[RESL-002]` Implement `StalledJobSentinel` watchdog thread to scan for orphaned active jobs and re-queue them.
- [x] `[RESL-003]` Handle worker process crash scenarios: ensure crashed jobs transition to retry or failed state.
- [x] `[RESL-004]` Implement exponential backoff retry calculation and delayed re-insertion into Redis.
- [x] `[RESL-005]` Implement dead-letter queue / permanent failure archiving when `attemptsMade >= attempts`.

---

## Phase 7: FlowProducer (Parent-Child DAG Workflows)

- [x] `[FLOW-001]` Define `FlowJob<T>` hierarchy representation for parent and child task trees.
- [x] `[FLOW-002]` Implement `FlowProducer.add(FlowJob tree)` to atomically push parent and child jobs into Redis.
- [x] `[FLOW-003]` Implement parent state transition to `WAITING_CHILDREN`.
- [x] `[FLOW-004]` Atomic Lua logic that decrements parent unresolved child counter and promotes parent to `WAITING` when count reaches zero.
- [x] `[FLOW-005]` Implement parent job access to child execution results map (`job.getChildrenValues()`).
- [x] `[FLOW-006]` Implement cascading failure options (`FAIL_PARENT` vs `IGNORE_AND_CONTINUE`).

---

## Phase 8: Traffic Shaping & Rate Limiting

- [x] `[RATE-001]` Port BullMQ Redis token bucket / sliding window rate limiter Lua script.
- [x] `[RATE-002]` Implement `RateLimiter` and `RateLimiterOptions` (`max`, `duration`, `groupKey`).
- [x] `[RATE-003]` Integrate rate limiter check before `moveToActive` worker acquisition.
- [x] `[RATE-004]` Implement automatic backoff when rate limit is exceeded without dropping jobs.

---

## Phase 9: Spring Boot 3.x Starter & Developer Annotations

- [x] `[SPRG-001]` Create `oxmq-spring-boot-starter` with Spring Boot 3.x autoconfiguration classes.
- [x] `[SPRG-002]` Build configuration property binding (`OxmqProperties`) for `application.yml`.
- [x] `[SPRG-003]` Reuse existing Spring `RedisConnectionFactory` / `LettuceConnectionFactory` bean if available.
- [x] `[SPRG-004]` Create `@EnableOxmq` annotation to activate automatic queue and worker scanning.
- [x] `[SPRG-005]` Create `@OxmqListener(queue = "...", concurrency = ...)` annotation for Spring beans.
- [x] `[SPRG-006]` Implement `OxmqListenerAnnotationBeanPostProcessor` to register and lifecycle-manage worker beans.
- [x] `[SPRG-007]` Provide injectable `Queue<T>` beans automatically.
- [x] `[SPRG-008]` Build Spring Boot Actuator health indicator (`OxmqHealthIndicator`).

---

## Phase 10: Performance Telemetry & Metrics (Micrometer)

- [x] `[METR-001]` Implement `OxmqMetrics` native telemetry engine wrapping `MeterRegistry`.
- [x] `[METR-002]` Counters: `oxmq.jobs.enqueued`, `oxmq.jobs.completed`, `oxmq.jobs.failed`, `oxmq.jobs.retried`, `oxmq.jobs.stalled`.
- [x] `[METR-003]` Gauges: `oxmq.jobs.active`, `oxmq.jobs.waiting`, `oxmq.jobs.delayed`.
- [x] `[METR-004]` Timers: `oxmq.job.duration` (latency percentiles p50/p95/p99), `oxmq.job.wait_time`.
- [x] `[METR-005]` Standalone and Spring Boot Actuator metrics export readiness.

---

## Phase 11: Production Hardening, Samples & Benchmarking

- [x] `[TEST-001]` Build JMH microbenchmark suite (`oxmq-benchmarks`).
- [x] `[TEST-002]` Build runnable standalone sample (`oxmq-sample-basic`).
- [x] `[TEST-003]` Build runnable Spring Boot 3 sample (`oxmq-sample-spring-boot`).
- [x] `[TEST-004]` Comprehensive Unit test suite across core models, serializers, backoffs, and metrics.
