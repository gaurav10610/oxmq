# Architectural Comparison: OxMQ vs. BullMQ
### *An Objective Engineering Evaluation of OxMQ (Java 17+ / Java 21 Loom) as the Premier Java Alternative to BullMQ*

**OxMQ** was created to bring the battle-tested, world-class queue architecture of **BullMQ** into the modern **Java 17+ / Java 21+ enterprise ecosystem**.

Because OxMQ directly executes BullMQ's official 49 Lua scripts and uses the exact same Redis key hierarchy, it achieves 100% functional and wire compatibility with BullMQ while unlocking the high-throughput advantages of Java 21's Project Loom Virtual Threads and enterprise Context Isolation.

---

## Comparison Matrix: OxMQ vs. BullMQ

| Feature / Capability | OxMQ (Java 17+ / Java 21+) | BullMQ (Node.js / TypeScript) |
| :--- | :---: | :---: |
| **Target Runtime** | **Java 17+ LTS** (OpenJDK, Temurin) — native **Java 21+ Project Loom** | **Node.js 16+** / TypeScript |
| **Concurrency Model** | **Project Loom Virtual Threads** (Java 21+) / Platform Thread Pool (Java 17) | Single-Threaded Event Loop |
| **I/O Blocking Mechanics** | Virtual threads unmount from carrier threads on blocking I/O | Non-blocking async/await promises |
| **Multi-Core CPU Scaling** | Natively concurrent across all JVM CPU cores | Requires spawning child OS processes ("sandboxed workers") |
| **Redis Lua Scripts** | **Direct execution of 49 official BullMQ Lua scripts (zero custom scripts)** | **Official BullMQ Lua scripts** |
| **Multi-Service Context Isolation** | **Native service-level namespace auto-inheritance (`oxmq.namespace`)** | Manual `prefix` configuration on every queue |
| **Virtual Queue Groups** | **Native Built-in ($O(1)$ fair circular round-robin, no pro tier)** | **Commercial Paywall (BullMQ Pro license required)** |
| **Intra-Group Priority Queuing** | **Native Built-in (`group.priority`, `getCountsPerPriorityForGroup`)** | **Commercial Paywall (BullMQ Pro)** |
| **Dynamic Per-Group Concurrency Overrides** | **Native Built-in (`setGroupConcurrency`, `getGroupConcurrency` via `groups:concurrency`)** | **Commercial Paywall (BullMQ Pro)** |
| **Group Concurrency Caps** | **Native Built-in (`groupConcurrency`)** | **Commercial Paywall (BullMQ Pro)** |
| **Group Rate Limiting** | **Native Built-in (`setGroupRateLimit(groupId, max, duration)`)** | **Commercial Paywall (BullMQ Pro)** |
| **Group Rate Limit TTL Query** | **Native Built-in (`getGroupRateLimitTtl(groupId, maxJobs)`)** | **Commercial Paywall (BullMQ Pro `getRateLimitTtl`)** |
| **Global Across-Groups Breakdown & Pagination** | **Native Built-in (`getGroups`, `getGroupsJobsCount`)** | **Commercial Paywall (BullMQ Pro `getGroupsJobsCount`)** |
| **Self-Healing Group Desynchronization Repair** | **Native Built-in (`repairGroup`, `repairMaxedGroups`)** | Manual script repair or BullMQ Pro support |
| **Reactive Stream Processors & Multi-Stage Checkpointing** | **Native Built-in (`reactiveProcessor`, `Flux`, `Mono`, `Flow.Publisher`)** | **Commercial Paywall (BullMQ Pro Observables)** |
| **Spring Boot Actuator Group REST Endpoints** | **Native Built-in (`/actuator/oxmq-groups` GET & POST management)** | None / Custom API development |
| **Opt-in Per-Group Prometheus Telemetry** | **Native Built-in (`oxmq.group-metrics-enabled=true`)** | None / Commercial add-on |
| **Batch Worker with Group Affinity** | **Native Built-in (`OxmqBatchWorker`, `job.getBatch()`, homogeneous tenant batches)** | **Commercial Paywall (BullMQ Pro `WorkerPro`, `job.getBatch()`)** |
| **Item-Level Granular Batch Failure** | **Native Built-in (`job.setAsFailed(err)`)** | **Commercial Paywall (BullMQ Pro)** |
| **Cluster-Wide Job Cancellation** | **Native Built-in (`cancelJob(id, reason)` over Redis `<queue>:control` pub/sub)** | **Manual custom code or BullMQ Pro** |
| **Resumable State Machines & Checkpointing** | **Native Built-in (`job.checkpoint(step, state)`, skips completed steps on retry)** | Requires manual database state persistence |
| **Declarative Per-Job-Type Timeouts** | **Native Built-in (`worker.jobTimeout(name, dur)` & `@JobTimeout`)** | Manual application-level timer wrappers |
| **Hierarchical Flow Tree Inspection** | **Native Built-in (`getFlow`, `job.getFlow`)** | **Commercial Paywall (BullMQ Pro FlowProducer)** |
| **Granular DAG Dependency Breakdown** | **Native Built-in (`getDependencies`, `job.getDependencies`)** | **Built-in `job.getDependencies`** |
| **Deduplication Key Query & Early Release** | **Native Built-in (`getDeduplicationJobId`, `removeDeduplicationKey`)** | Built-in `removeDeduplicationKey` |
| **Self-Contained Connected Job Domain Object** | **Native Built-in (`job.updateData`, `job.promote`, `job.changeDelay`, `job.changePriority`, `job.retry`, `job.remove`, `job.clearLogs`)** | Standard `Job` methods in BullMQ |
| **Dynamic Runtime Child Spawning in DAGs** | **Native Built-in (`job.addChild` / `job.addChildren`, yields virtual thread to `waiting-children`)** | **Commercial Paywall (BullMQ Pro Flows)** |
| **Distributed Pub/Sub Control Plane / Relay** | **Native Built-in (`OxmqRelay`, hierarchical wildcards `*` / `>`, retained messages, node leases)** | Requires external NATS / broker / custom pubsub |
| **Hard Watchdog Execution Timeouts** | **Native Built-in (`jobTimeout(Duration)`)** | Manual application-level timer wrappers |
| **Transactional Outbox Engine** | **Native Built-in (`@EnableOxmqOutbox`, `OxmqOutboxService`)** | Requires external CDC (Debezium) or custom code |
| **Request-Reply Pattern** | **Native `waitUntilFinished` & `waitUntilFinishedAsync` (Redis Streams)** | `Job.waitUntilFinished` (Redis Streams) |
| **Memory Retention (`KeepJobs`)** | **Built-in `KeepJobs` (`count`, `ageSeconds`, `limit`) via `moveToFinished-14.lua`** | Built-in `keepJobs` via `moveToFinished-14.lua` |
| **Strict Redis Memory Ceiling** | **Supported via `JobInterceptor` + `moveToDelayed(skipAttempt: true)`** | Not supported (Redis LRU eviction destroys active queues) |
| **Redis Key Topology** | Standard `bull:<queue>:*` hierarchy (including `dn:` & `pc`) | Standard `bull:<queue>:*` hierarchy |
| **Job Lifecycle States** | WAITING, ACTIVE, DELAYED, WAITING_CHILDREN, COMPLETED, FAILED, PAUSED, STALLED | WAITING, ACTIVE, DELAYED, WAITING_CHILDREN, COMPLETED, FAILED, PAUSED, STALLED |
| **Parent-Child DAG Workflows** | Built-in `FlowProducer` (leaf dedup, auto-reattachment, `addBulk`) | Built-in `FlowProducer` (leaf dedup, auto-reattachment, `addBulk`) |
| **Sliding-Window Rate Limiting**| Built-in token bucket with `groupKey` | Built-in token bucket with `groupKey` |
| **Job Deduplication & Buffering**| Built-in (`jobId`, `deduplicationId`, `keepLastIfActive` buffer) | Built-in (`jobId`, `deduplicationId`, `keepLastIfActive` buffer) |
| **Worker Postponement** | `DelayedException` (zero attempt consumption) | `DelayedError` (skipAttempt) |
| **Priority Preservation on Retry**| Yes (`reprocessJob-9.lua`) | Yes (`reprocessJob-9.lua`) |
| **Storage Backend Contract** | Pluggable `QueueBackend` (`RedisQueueBackend`) | Pluggable `IQueueBackend` (`RedisBackend`) |
| **Queue Range Pagination & Inspection** | Built-in `getJobs(states, start, end, asc)`, `getWaiting`, `getActive`, `getJobCounts` | Built-in `getJobs(types, start, end, asc)`, `getJobCounts` |
| **Global Concurrency & Rate Limit Metadata** | Built-in `setGlobalConcurrency`, `setGlobalRateLimit`, `isMaxed` | Built-in `setGlobalConcurrency`, `setGlobalRateLimit`, `isMaxed` |
| **Pluggable Custom Backoff Registry** | Dynamic `BackoffStrategy.custom(...)` + worker registry | Custom backoff strategies via worker settings |
| **Programmatic Job Discarding** | `job.discard()` (aborts retries and fails immediately) | `job.discard()` |
| **Ignored Children Failures** | `job.getIgnoredChildrenFailures()` on parent DAG jobs | `job.getIgnoredChildrenFailures()` |
| **Worker Event Emitter** | Fluent `worker.onActive`, `onCompleted`, `onFailed`, etc. | `worker.on('active')`, `worker.on('completed')`, etc. |
| **Distributed Tracing (OpenTelemetry)** | Built-in W3C `TraceContext` (`traceparent`, `tracestate`) | BullMQ OpenTelemetry instrumentation / telemetry opts |
| **Live Progress & Step Logs** | Built-in (`job.updateProgress`, `job.log`) | Built-in (`job.updateProgress`, `job.log`) |
| **Web Dashboard** | Native **Bull-Board UI** compatibility (Zero Node.js code) | Native **Bull-Board UI** compatibility |
| **Framework Integration** | **Spring Boot 3+ Starter** (`@OxmqListener`, `@OxmqBatchListener`, Actuator) | Express, Fastify, NestJS BullMQ integration |
| **Pricing & Pro Tier** | **100% Free & Open Source (NO PRO TIER, NO PAYWALL)** | **Commercial BullMQ Pro (annual license subscription)** |
| **License** | **Apache 2.0** | **MIT (Community) / Commercial Proprietary (Pro)** |

---

## The Distributed Queue Landscape

When evaluating task processing in enterprise systems, teams frequently compare OxMQ to brokers like **Kafka**, **RabbitMQ**, and Python's **Celery**:

| Dimension | OxMQ (Java 17/21) | BullMQ (Node.js) | Celery (Python) | Kafka / RabbitMQ |
| :--- | :---: | :---: | :---: | :---: |
| **Primary Domain** | Distributed Jobs & DAGs | Distributed Jobs & DAGs | Background Tasks | Event Streaming / Messaging |
| **Delayed Execution** | Native (Redis ZSET) | Native (Redis ZSET) | In-memory ETA | Requires custom DLX / plugins |
| **Parent-Child DAGs** | Native `FlowProducer` | Native `FlowProducer` | Canvas primitives | External orchestrator needed |
| **Rate Limiting** | Token Bucket + Grouping | Token Bucket + Grouping | External extension | Downstream consumer throttling |
| **Request-Reply** | Native `waitUntilFinished` | Native `waitUntilFinished` | Backend result polling | Complex correlation ID routing |
| **Web UI** | Instant Bull-Board UI | Instant Bull-Board UI | Flower | Confluent / RabbitMQ Mgmt |
| **Concurrency Engine** | Virtual Threads (Loom) | Single-thread Event Loop | Gevent / Fork processes | Multi-threaded consumers |
| **Operational Simplicity** | Reuses existing Redis | Reuses existing Redis | Requires Redis + Result DB | Heavy broker cluster maintenance |

---

## In-Depth Architectural Analysis

### 1. Concurrency: Project Loom Virtual Threads vs. Node.js Event Loop
* **BullMQ in Node.js**:
  - Operates on Node.js's single-threaded event loop.
  - Excellent for high-volume non-blocking asynchronous network I/O.
  - However, if tasks perform heavy CPU work (e.g. image processing, cryptography, PDF generation, large JSON parsing), the single thread can become blocked. BullMQ solves this by spawning child OS processes ("sandboxed workers"), which introduces inter-process communication (IPC) overhead.
* **OxMQ in Java 17+ & Java 21+**:
  - Operates on native **Project Loom Virtual Threads** on Java 21+ (`Executors.newVirtualThreadPerTaskExecutor()`) and dynamic platform thread pools on Java 17 LTS via `DispatcherExecutorFactory`.
  - When tasks perform blocking calls (JDBC database queries, third-party REST calls, blocking socket I/O), the virtual thread unmounts from its underlying OS carrier thread with near-zero memory footprint (~1 KB per thread).
  - CPU-bound tasks execute concurrently across all available CPU cores on the JVM without needing separate OS child processes.

---

### 2. Multi-Service Microservice Namespacing
* **BullMQ**:
  - Requires manually passing `{ prefix: 'my-service' }` when instantiating every `Queue`, `Worker`, and `QueueEvents` instance across your codebase.
* **OxMQ**:
  - Automatically derives the namespace from `spring.application.name` or `oxmq.namespace` in `application.yml`.
  - Guarantees zero cross-service collision on shared Redis clusters while preserving Redis Cluster single-slot hash tags (`{prefix:queueName}`).

---

### 3. Polyglot Interoperability
* **Shared Redis Topology**:
  - OxMQ and BullMQ share the identical Redis key schema, sorted sets, lists, hashes, and streams.
  - This unlocks seamless cross-language workflows: a Node.js frontend can enqueue jobs into Redis with BullMQ, and an OxMQ worker in Java can process them with Virtual Threads, or vice versa.
* **Unified Dashboard**:
  - You can monitor both Java OxMQ queues and Node.js BullMQ queues side-by-side on the same **Bull-Board UI** instance.

---

## Measuring Performance in Your Environment

Because throughput and latency depend heavily on network round-trip time (RTT), payload size, Redis persistence configuration (AOF vs RDB), and worker business logic, **we encourage running empirical benchmarks**:

OxMQ includes a dedicated **JMH (Java Microbenchmark Harness)** module in [`oxmq-benchmarks/`](https://github.com/gaurav10610/oxmq/tree/main/oxmq-benchmarks):

```bash
# Compile benchmark suite
./mvnw clean test-compile -pl oxmq-benchmarks

# Run benchmarks against your local or remote Redis instance
java -jar oxmq-benchmarks/target/oxmq-benchmarks-1.9.0.jar
```

---

## Attribution

OxMQ proudly stands on the shoulders of giants. We express our sincere appreciation to the open-source **[BullMQ](https://github.com/taskforcesh/bullmq)** project and its community for originating and maintaining world-class Redis queue architectures under the permissive MIT license. OxMQ directly reuses the official BullMQ Lua scripts to deliver 100% wire and functional parity in the Java ecosystem.
