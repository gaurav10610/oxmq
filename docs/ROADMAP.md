# Project Roadmap

**Project:** `OxMQ` (Distributed Job & Workflow Engine for Java)  
**Version Strategy:** Semantic Versioning (`v0.1.0` $\rightarrow$ `v1.0.0`)  
**Target Completion:** 6 Milestones  

---

## Milestone Timeline Overview

```
  Q1: Foundation & Core               Q2: Workflows & Frameworks            Q3: GA & Scale
┌───────────────────────────────┐   ┌───────────────────────────────┐   ┌───────────────────────────────┐
│ Milestone 1: Core Wire-Compat │   │ Milestone 3: Flows & DAGs     │   │ Milestone 5: UI & Metrics     │
│ Milestone 2: Concurrency/Loom │   │ Milestone 4: Spring Boot      │   │ Milestone 6: Hardening & GA   │
└───────────────────────────────┘   └───────────────────────────────┘   └───────────────────────────────┘
```

---

## Milestone 1: Foundation & Redis Wire-Compatibility (`v0.1.0`)
**Primary Goal:** Establish the core project repository, Redis connection layer via Lettuce, and foundational BullMQ Lua scripts for atomic job enqueueing, fetching, and completion.

* **Key Deliverables:**
  * Multi-module Maven project structure (`oxmq-core`, `oxmq-spring-boot-starter`, `oxmq-benchmarks`, `oxmq-samples`).
  * Porting core BullMQ Lua scripts (`addJob.lua`, `moveToActive.lua`, `moveToFinished.lua`, `retryJob.lua`, `extendLock.lua`, `cleanQueue.lua`, `pauseQueue.lua`, `rateLimit.lua`).
  * Ergonomic `Queue<T>`, `Worker<T>`, and `Job<T>` builder APIs.
  * Pluggable Jackson serialization engine (`JobSerializer`) supporting Java Records and JSR-310 time types.
  * Native Micrometer metrics foundation.

---

## Milestone 2: Concurrency, Virtual Threads & Robustness (`v0.2.0`)
**Primary Goal:** Build the high-throughput task execution engine with first-class Java 21 Virtual Threads (Loom) and production-grade resilience.

* **Key Deliverables:**
  * Virtual Thread-per-task dispatcher (`Executors.newVirtualThreadPerTaskExecutor()`) with platform thread fallback.
  * Configurable worker concurrency limits (e.g. 50–5000 concurrent jobs).
  * Stalled job sentinel (watchdog thread scanning for dead/orphaned worker locks).
  * Heartbeat / lock extension mechanism for long-running jobs.
  * Retry policies with Exponential, Fixed, and Custom Backoff formulas.
  * Real-time progress updates (`job.updateProgress(int percentage)`).
  * Graceful shutdown hooks with configurable drain timeout.

---

## Milestone 3: Advanced Scheduling, Rate Limiting & Flow Engine (`v0.3.0`)
**Primary Goal:** Implement BullMQ's most powerful features: parent-child job hierarchies (DAGs) and distributed rate limiting.

* **Key Deliverables:**
  * `FlowProducer` engine: Define job trees where parent jobs wait for child completion.
  * Child-to-parent result injection and cascading failure handlers.
  * Token-bucket and sliding-window rate limiters per queue/key.
  * Repeatable / Cron job scheduler with IANA timezone support.
  * Job deduplication with debounce windows and custom ID keys.
  * Pause, resume, and obliterate queue controls.

---

## Milestone 4: Spring Boot Integration & Developer Ergonomics (`v0.4.0`)
**Primary Goal:** Provide a seamless, zero-friction developer experience for Spring Boot 3.x applications.

* **Key Deliverables:**
  * `@EnableOxmq` annotation and Spring Boot Auto-Configuration (`OxmqAutoConfiguration`).
  * Reuse existing `RedisConnectionFactory` / `LettuceConnectionFactory` from Spring Data Redis.
  * Declarative `@OxmqListener(queue = "name", concurrency = 50)` worker annotation.
  * Automatic JSON payload conversion into strongly typed DTOs.
  * Spring Environment configuration bindings (`application.yml` / `application.properties`).
  * Actuator health indicators and endpoint integration.

---

## Milestone 5: Observability, Metrics & Embedded UI (`v0.5.0`)
**Primary Goal:** Provide instant visual inspection and enterprise metrics integration.

* **Key Deliverables:**
  * **Bull-Board Verification:** 100% interoperability with the Node.js [Bull-Board](https://github.com/felixmosh/bull-board) UI.
  * **Micrometer Metrics:** Export real-time gauges, counters, and timers for queue latency, active workers, failure rates, and retry counts.
  * OpenTelemetry tracing propagation for distributed trace context across job boundaries.

---

## Milestone 6: Benchmarking, Hardening & 1.0.0 GA (`v1.0.0`)
**Primary Goal:** Production readiness, performance validation, and official public release.

* **Key Deliverables:**
  * JMH microbenchmarking suite for enqueue and worker throughput.
  * Polyglot verification: Java Producer $\rightarrow$ Node Worker, Node Producer $\rightarrow$ Java Worker.
  * Full Javadoc, website documentation, Quickstart guides, and sample projects.
  * Publication to Maven Central (`io.oxmq:oxmq-core`, `io.oxmq:oxmq-spring-boot-starter`).
