# 📋 Product Requirements & Performance Specification (PRD)

**Project Name:** `OxMQ`  
**Document Version:** `1.1.0` (Consolidated)  
**Status:** In Active Implementation  
**License:** Apache 2.0 (100% Open Source, No Paywalls)  
**Target Runtime:** Java 21+ LTS (Native Virtual Threads / Loom) with Java 17 Compatibility  

---

## 1. Executive Summary & Vision

### 1.1 The Vision
To build the **fastest, most developer-friendly, 100% open-source distributed job queue and workflow engine for the modern Java ecosystem**. It combines the raw speed and atomic Lua-script architecture of **BullMQ** with the unmatched concurrency of **Java 21 Virtual Threads (Project Loom)**, native **Micrometer performance telemetry**, and frictionless **Spring Boot auto-configuration**.

### 1.2 Core Value Propositions
1. **100% Open Source Without Paywalls:** Provide mission-critical enterprise features (Parent-Child DAG Workflows, Token-Bucket Rate Limiting, Dynamic Queues, Sub-second Delays) completely free, eliminating the costly commercial paywalls imposed by tools like JobRunr Pro.
2. **Virtual Thread Native (Loom-First):** Enable thousands of concurrent I/O-bound background workers (HTTP webhooks, DB queries, LLM calls) with near-zero memory footprint and no thread-pool exhaustion.
3. **BullMQ Wire-Compatibility & Polyglot Interop:** Use the battle-tested Redis schema and Lua scripts of BullMQ so Java, Node.js, and Python microservices can seamlessly produce and consume jobs across the same Redis cluster, with instant compatibility for the widely used **Bull-Board** UI.
4. **Frictionless Embeddability & Minimal Setup:** Plugs directly into existing Spring Boot applications by reusing existing `Lettuce` Redis connections, `Jackson` object mappers, and `Micrometer` metrics without introducing heavy transitive dependencies. Usable in standalone Java in under 5 lines of code.
5. **Native Performance Metrics:** Native, out-of-the-box telemetry hooks for Micrometer, Prometheus, Grafana, and OpenTelemetry to observe queue depth, latency distributions (p50, p95, p99), worker throughput, and error rates without additional plugins.

---

## 2. Competitive Landscape & Market Comparison

| Feature / Dimension | Quartz Scheduler | db-scheduler | JobRunr (Free / Pro) | Redisson RQueue | **OxMQ (This Project)** |
| :--- | :--- | :--- | :--- | :--- | :--- |
| **Primary Storage** | JDBC / RDBMS | JDBC / RDBMS | Storage-Agnostic | Redis | **Pure Redis (6.2+ / 7.x)** |
| **Execution Latency** | Polling (1-5s) | Polling (1-5s) | Polling (1-5s) | Sub-millisecond | **Sub-millisecond (&lt; 1ms)** |
| **Max Throughput** | ~ 400 ops/s | ~ 850 ops/s | ~ 2,200 ops/s | ~ 18,000 ops/s | **&ge; 25,000 ops/s** |
| **Parent-Child DAGs** | ❌ No | ❌ No | 💳 **Paid Pro Only** | ❌ No | **✅ 100% Free / Native** |
| **Rate Limiting** | ❌ No | ❌ No | 💳 **Paid Pro Only** | ❌ Manual | **✅ 100% Free (Sliding Window)** |
| **Web Dashboard** | ❌ None | ❌ None | ✅ Included | ❌ None | **✅ Embedded + Bull-Board UI** |
| **Polyglot Interop** | ❌ Java only | ❌ Java only | ❌ Java only | ❌ Java only | **✅ Node.js / Python / Java** |
| **Concurrency Model** | Heavy OS Threads | Heavy OS Threads | Thread Pool | Thread Pool | **✅ Java 21 Virtual Threads (Loom)** |
| **Telemetry & Metrics** | ❌ Plugin | ❌ Plugin | ⚠️ Basic | ⚠️ Basic | **✅ Native Micrometer (p99 Timers)** |

---

## 3. Functional Requirements (Features)

### 3.1 Core Job Lifecycle Management
* **FR-01: Job State Machine:** Support full lifecycle states: `WAITING`, `ACTIVE`, `DELAYED`, `COMPLETED`, `FAILED`, `PAUSED`, `STALLED`, `WAITING_CHILDREN`.
* **FR-02: Delayed Execution:** Schedule jobs with millisecond-accurate delay using Redis Sorted Sets (`ZADD` with timestamp score).
* **FR-03: Automatic Retries & Backoff:** Configurable retry attempts with strategies:
  * `FIXED` (e.g. retry every 5s)
  * `EXPONENTIAL` (e.g. $1\text{s} \times 2^{\text{attempt}}$, capped at max duration)
  * `CUSTOM` (user-defined backoff function)
* **FR-04: Deduplication & Idempotency:** Custom `jobId` assignment to prevent duplicate job insertion within a configurable debounce/TTL window.
* **FR-05: Real-time Progress Tracking:** Workers can call `job.updateProgress(int percentage)` or publish structured progress payloads observable via Redis Pub/Sub.
* **FR-06: Job Logging & Return Values:** Persist JSON-serialized job input arguments, return values, and step logs (`job.log(msg)`) in Redis.

### 3.2 Concurrency & Worker Dispatcher
* **FR-07: Virtual Thread Concurrency:** Worker pools default to Java 21 `Executors.newVirtualThreadPerTaskExecutor()` with configurable semaphore concurrency limits.
* **FR-08: Stalled Job Sentinel:** Background watchdog thread to detect stalled jobs (worker crashed or hung without heartbeat) and automatically re-queue them up to max retry limits.
* **FR-09: Lock Extender Heartbeat:** Automatic background lock renewal for long-running jobs.
* **FR-10: Graceful Shutdown:** Configurable drain timeout to allow active jobs to finish processing when `SIGTERM` / `contextClose` occurs.

### 3.3 Advanced Workflows (FlowProducer / DAGs)
* **FR-11: Parent-Child Job Trees:** Define hierarchical job trees where a parent job transitions to `WAITING_CHILDREN` and automatically activates only when all child jobs succeed.
* **FR-12: Child Result Propagation:** Parent job has access to the returned results of all its completed child jobs.
* **FR-13: Cascading Failure Policies:** Configurable behavior when a child fails (`FAIL_PARENT` vs `CONTINUE_WITH_PARTIAL`).

### 3.4 Traffic Shaping & Rate Limiting
* **FR-14: Sliding Window Rate Limiter:** Enforce maximum $N$ jobs per $X$ duration (e.g., max 100 requests / minute) across distributed workers using atomic Redis token buckets.
* **FR-15: Queue Pause / Resume:** Dynamically pause job consumption across the entire cluster and resume on demand.

### 3.5 Developer Experience & Integrations
* **FR-16: Spring Boot Starter:** Automatic configuration via `application.yml` and `@OxmqListener` annotation processing.
* **FR-17: Standalone / Core Mode:** Zero Spring dependencies in `oxmq-core`; usable in plain Java, CLI apps, Quarkus, or Micronaut in under 5 lines of code.
* **FR-18: Dashboard & Monitoring:**
  * Out-of-the-box compatibility with [Bull-Board](https://github.com/felixmosh/bull-board).
  * Actuator health indicators and endpoint integration.

---

## 4. Performance & Non-Functional Requirements (NFRs)

| Metric / Requirement | Specification Target | Verification Method |
| :--- | :--- | :--- |
| **NFR-01: Enqueue Throughput** | $\ge 35,000$ jobs/sec | JMH single Redis standalone node, batch size 100 |
| **NFR-02: Worker Execution Throughput** | $\ge 20,000$ jobs/sec (I/O bound) | JMH benchmark with 1,000 Virtual Threads on Java 21 |
| **NFR-03: State Transition Latency (p99)** | $\le 1.5\text{ ms}$ | Micrometer timer histograms (`oxmq.job.duration`) |
| **NFR-04: Memory Footprint per Worker** | $\le 2\text{ KB}$ | Java 21 Virtual Thread baseline memory allocation |
| **NFR-05: High Concurrency Soak Stability**| Zero memory or connection leaks over 72h soak | Continuous integration soak tests |
| **NFR-06: Redis Topology Support** | Standalone, Sentinel, Cluster, AWS ElastiCache | Lettuce cluster & sentinel connection drivers |
| **NFR-07: Bytecode Hygiene** | Minimal dependencies (`lettuce-core`, `jackson-databind`, `slf4j-api`, `micrometer-core`) | Maven dependency tree analysis |
