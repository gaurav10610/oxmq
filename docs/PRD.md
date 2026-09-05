# Product Requirements Document (PRD)

**Project Name:** `OxMQ`  
**Document Version:** `1.0.0`  
**Status:** In Active Implementation  
**License Target:** Apache 2.0 (100% Open Source, No Paywalled Core Features)  
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

## 2. Market Gap & Competitive Analysis

| Dimension | Quartz Scheduler | db-scheduler | JobRunr (Free / Pro) | Redisson | **OxMQ (This Project)** |
| :--- | :--- | :--- | :--- | :--- | :--- |
| **Primary Storage** | JDBC / RDBMS | JDBC / RDBMS | Storage-Agnostic | Redis | **Pure Redis (6.2+)** |
| **Execution Latency** | Polling (Seconds) | Polling (Seconds) | Polling (1-5s) | Sub-millisecond | **Sub-millisecond (Push/Streams)** |
| **Throughput Target** | < 500 jobs/sec | < 1,000 jobs/sec | ~ 2,000 jobs/sec | > 20,000 jobs/sec | **> 25,000 jobs/sec** |
| **Parent-Child DAGs** | ❌ No | ❌ No | 💳 **Paid Pro Only** | ❌ No | **✅ 100% Free / Native** |
| **Rate Limiting** | ❌ No | ❌ No | 💳 **Paid Pro Only** | ❌ Manual | **✅ 100% Free (Sliding Window)** |
| **Web Dashboard** | ❌ None | ❌ None | ✅ Included | ❌ None | **✅ Embedded + Bull-Board** |
| **Polyglot Interop** | ❌ Java only | ❌ Java only | ❌ Java only | ❌ Java only | **✅ Node.js / Python / Java** |
| **Virtual Threads** | ❌ Legacy | ❌ Thread pools | ⚠️ Partial | ⚠️ Partial | **✅ Loom Native First-Class** |
| **Native Metrics** | ❌ Plugin | ❌ Plugin | ⚠️ Basic | ⚠️ Basic | **✅ Built-in Micrometer Suite** |

---

## 3. Target Personas & Primary Use Cases

### 3.1 Target Personas
* **Spring Boot / Java Backend Engineers:** Wanting an out-of-the-box, reliable background job queue with annotation-based listeners (`@OxmqListener`), delayed jobs, retries, and a dashboard without paying for JobRunr Pro.
* **Polyglot Microservices Architects:** Teams running Node.js/Next.js frontends, Python AI workers, and Java core services needing a shared, lightweight task distribution layer without deploying heavyweight Kafka or Temporal clusters.
* **High-Throughput SaaS Developers:** Needing strict rate limiting per tenant/queue, webhook dispatchers, scheduled recurring jobs, and complex multi-step processing pipelines (DAGs).

### 3.2 Primary Use Cases
1. **Asynchronous Webhooks & Notifications:** Sending emails, push notifications, and webhooks with automatic exponential backoff retries and rate limiting per provider.
2. **Multi-Stage Data Pipelines (DAG Workflows):** Ingesting a file $\rightarrow$ chunking data $\rightarrow$ dispatching 50 parallel child worker jobs $\rightarrow$ parent job aggregating results upon completion.
3. **Scheduled & Recurring Jobs:** Cron-like jobs with millisecond precision and distributed deduplication.
4. **AI & LLM Task Offloading:** Dispatching long-running LLM generation requests across thousands of lightweight Virtual Threads.

---

## 4. Technical Architecture & Component Design

```
+-----------------------------------------------------------------------------------+
|                           User Application Layer                                 |
|             (Spring Boot / Quarkus / Micronaut / Standalone Java)                 |
+-----------------------------------------------------------------------------------+
                                         │
                 ┌───────────────────────┴───────────────────────┐
                 ▼                                               ▼
+----------------------------------+            +----------------------------------+
|      oxmq-spring-boot-starter    |            |            oxmq-core             |
|  - @EnableOxmq                   |            |  - Queue<T> & Worker<T>          |
|  - @OxmqListener annotations     |            |  - FlowProducer (DAG Engine)     |
|  - Spring Data Redis reuse       |            |  - Virtual Thread Dispatcher     |
|  - Actuator Endpoint & Health    |            |  - Stalled Job Watchdog          |
|  - OxmqProperties auto-bind      |            |  - OxmqMetrics (Micrometer)      |
+----------------------------------+            +----------------------------------+
                 │                                               │
                 └───────────────────────┬───────────────────────┘
                                         ▼
+-----------------------------------------------------------------------------------+
|                                  Engine Layer                                     |
|  - Redis Driver   : Lettuce 6.x (Async, Reactive, Sentinel, Cluster native)       |
|  - Serialization  : Pluggable (Jackson 2.x default with JavaTime & Records)       |
|  - Telemetry      : Native Micrometer (Prometheus, Grafana, OpenTelemetry)        |
|  - Lua Engine     : BullMQ v5 Atomic Lua Scripts (addJob, moveToActive, etc.)     |
+-----------------------------------------------------------------------------------+
                                         │
                                         ▼
+-----------------------------------------------------------------------------------+
|                               Redis Storage Engine                                |
|   - bull:<queue>:wait (List / Stream)      - bull:<queue>:delayed (Sorted Set)    |
|   - bull:<queue>:active (List)             - bull:<queue>:completed / failed     |
|   - bull:<queue>:<jobId> (Hash)            - bull:<queue>:events (Pub/Sub)        |
|   - bull:<queue>:stalled (Watchdog)        - bull:<queue>:limiter (Rate Limiting) |
+-----------------------------------------------------------------------------------+
```

---

## 5. Functional Requirements (Features)

### 5.1 Core Job Lifecycle Management
* **FR-01: Job State Machine:** Support full lifecycle states: `WAITING`, `ACTIVE`, `DELAYED`, `COMPLETED`, `FAILED`, `PAUSED`, `STALLED`.
* **FR-02: Delayed Execution:** Schedule jobs with millisecond-accurate delay using Redis Sorted Sets (`ZADD` with timestamp score).
* **FR-03: Automatic Retries & Backoff:** Configurable retry attempts with strategies:
  * `FIXED` (e.g. retry every 5s)
  * `EXPONENTIAL` (e.g. $1\text{s} \times 2^{\text{attempt}}$)
  * `CUSTOM` (user-defined backoff function)
* **FR-04: Deduplication & Idempotency:** Custom `jobId` assignment to prevent duplicate job insertion within a configurable debounce/TTL window.
* **FR-05: Real-time Progress Tracking:** Workers can call `job.updateProgress(int percentage)` or publish structured progress payloads observable via Redis Pub/Sub.
* **FR-06: Job Data & Return Values:** Persist JSON-serialized job input arguments and worker return values in the job hash (`bull:<queue>:<jobId>`).

### 5.2 Concurrency & Worker Dispatcher
* **FR-07: Virtual Thread Concurrency:** Worker pools default to Java 21 `Executors.newVirtualThreadPerTaskExecutor()` with configurable max concurrency limits.
* **FR-08: Stalled Job Sentinel:** Background watchdog thread to detect stalled jobs (worker crashed or hung without heartbeat) and automatically re-queue them up to max retry limits.
* **FR-09: Graceful Shutdown:** Configurable drain timeout to allow active jobs to finish processing when `SIGTERM` / `contextClose` occurs.

### 5.3 Advanced Workflows (FlowProducer / DAGs)
* **FR-10: Parent-Child Job Trees:** Define hierarchical job trees where a parent job transitions to `WAITING_CHILDREN` and automatically activates only when all child jobs succeed.
* **FR-11: Child Result Propagation:** Parent job has access to the returned results of all its completed child jobs.
* **FR-12: Cascading Failure Policies:** Configurable behavior when a child fails (`FAIL_PARENT` vs `CONTINUE_WITH_PARTIAL`).

### 5.4 Traffic Shaping & Rate Limiting
* **FR-13: Sliding Window Rate Limiter:** Enforce maximum $N$ jobs per $X$ duration (e.g., max 100 requests / minute) across distributed workers using atomic Redis token buckets.
* **FR-14: Queue Pause / Resume:** Dynamically pause job consumption across the entire cluster and resume on demand.

### 5.5 Developer Experience & Integrations
* **FR-15: Spring Boot Starter:** Automatic configuration via `application.yml` and `@OxmqListener` annotation processing.
* **FR-16: Standalone / Core Mode:** Zero Spring dependencies in `oxmq-core`; usable in plain Java, CLI apps, Quarkus, or Micronaut in under 5 lines of code.
* **FR-17: Dashboard & Monitoring:**
  * Out-of-the-box compatibility with [Bull-Board](https://github.com/felixmosh/bull-board).
  * Actuator endpoints and embeddable health indicators.
* **FR-18: Native Performance Telemetry (Micrometer):**
  * Export counters, gauges, and timers for queue latency, active workers, failure rates, retry counts, and wait times.

---

## 6. Non-Functional Requirements (Performance & Reliability)

* **NFR-01: High Throughput:** Core engine must handle $\ge 20,000$ job enqueues/sec and $\ge 15,000$ job completions/sec on standard Redis hardware.
* **NFR-02: Low Latency:** Job fetch and state transition latency $\le 2\text{ms}$ under normal load.
* **NFR-03: Zero Leaks:** No connection leaks, memory leaks, or thread leaks over 72-hour continuous soak tests under high concurrency.
* **NFR-04: Redis Topology Support:** Full support for Redis Standalone, Redis Sentinel, Redis Cluster, AWS ElastiCache, and Redis Cloud.
* **NFR-05: Strict Bytecode Hygiene:** Core module has minimal transitive dependencies (`lettuce-core`, `slf4j-api`, `jackson-databind`, `micrometer-core`).
