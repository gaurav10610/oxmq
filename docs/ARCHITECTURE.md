# 🏛️ OxMQ Architecture & Internals

This document details the architectural design, Redis data structures, concurrency model, DAG execution engine, Batch Dequeue pipeline, and performance telemetry in **OxMQ**.

---

## 1. High-Level System Architecture

OxMQ is engineered around 6 core pillars:
1. **Atomic Redis State Transitions:** All state transitions (`WAITING` $\rightarrow$ `ACTIVE` $\rightarrow$ `COMPLETED` / `FAILED` / `DELAYED`) execute via official BullMQ Lua scripts with single-roundtrip ACID guarantees.
2. **Virtual Thread Native with Java 17 Fallback:** Every task execution runs in an ultra-lightweight Java 21 Virtual Thread (with automatic fallback to high-performance platform thread pools on Java 17 LTS via `DispatcherExecutorFactory`), enabling 1,000+ to 10,000+ concurrent I/O-bound workers with near-zero memory overhead.
3. **High-Throughput Batch Dequeue:** Bulk pop up to $N$ jobs atomically in 1 Redis call for fast database ingestion (ClickHouse, Elasticsearch, PostgreSQL batch inserts).
4. **100% BullMQ v6 Wire & Script Parity:** Reuses official, battle-tested BullMQ v6 Lua scripts with zero custom Lua code. Standard BullMQ Redis key hierarchies enable seamless polyglot interoperability with Node.js/Python workers and instant compatibility with **Bull-Board UI**.
5. **Pluggable Storage Backend (`QueueBackend`):** Mirrors BullMQ v6's internal backend abstraction (`io.oxmq.backend.QueueBackend`), decoupling high-level queue operations from the Lettuce Redis connection driver (`RedisQueueBackend`).
6. **Native Performance Instrumentation:** Microsecond-resolution Micrometer timers, gauges, and counters built into the core path.

```mermaid
graph TB
    subgraph AppLayer["Application Layer"]
        Producer["OxmqQueue&lt;T&gt;<br/>(Producer)"]
        Flow["FlowProducer<br/>(DAG Workflows)"]
        SpringWorker["@OxmqListener<br/>(Spring Boot 3)"]
        CoreWorker["OxmqWorker&lt;T&gt;<br/>(1-by-1 Loom Worker)"]
        BatchWorker["OxmqBatchWorker&lt;T&gt;<br/>(Bulk Ingestion Worker)"]
        Events["QueueEvents<br/>(Pub/Sub Listener)"]
    end

    subgraph OxmqCore["OxMQ Core Engine"]
        LoomDispatcher["Virtual Thread Dispatcher<br/>(Executors.newVirtualThreadPerTaskExecutor)"]
        LockWatchdog["LockExtender &amp; StalledJobSentinel<br/>(Background Watchdog)"]
        RateLimit["RateLimiter<br/>(Sliding Window Token Bucket)"]
        MetricsEngine["OxmqMetrics<br/>(Native Micrometer Integration)"]
        LuaManager["LuaScriptManager<br/>(SHA-1 Digest Caching &amp; EVALSHA)"]
    end

    subgraph RedisStorage["Redis Storage Engine (BullMQ Wire-Compatible)"]
        WaitList[("bull:&lt;queue&gt;:wait<br/>[FIFO List]")]
        ActiveList[("bull:&lt;queue&gt;:active<br/>[Active List]")]
        DelayedZSet[("bull:&lt;queue&gt;:delayed<br/>[Timestamp ZSet]")]
        CompletedZSet[("bull:&lt;queue&gt;:completed<br/>[Completed ZSet]")]
        FailedZSet[("bull:&lt;queue&gt;:failed<br/>[Failed ZSet]")]
        JobHashes[("bull:&lt;queue&gt;:&lt;id&gt;<br/>[Job Metadata Hash]")]
        EventsPubSub[("bull:&lt;queue&gt;:events<br/>[Pub/Sub Channel]")]
        LimiterKey[("bull:&lt;queue&gt;:limiter<br/>[Sliding Window ZSet]")]
    end

    subgraph TelemetrySink["Observability Sinks"]
        Prometheus["Prometheus / Grafana"]
        BullBoardUI["Bull-Board UI Dashboard"]
    end

    Producer -->|add / addBulk| LuaManager
    Flow -->|add DAG tree| LuaManager
    SpringWorker --> LoomDispatcher
    CoreWorker --> LoomDispatcher
    BatchWorker --> LoomDispatcher

    LoomDispatcher --> LuaManager
    LockWatchdog --> LuaManager
    RateLimit --> LuaManager

    LuaManager --> RedisStorage
    MetricsEngine --> Prometheus
    EventsPubSub --> Events
    EventsPubSub --> BullBoardUI
    JobHashes --> BullBoardUI
```

---

## 2. Job Lifecycle State Machine

Jobs transition through a strictly enforced, atomic state machine managed by official BullMQ Redis Lua scripts:

<p align="center">
  <img src="/assets/oxmq-job-lifecycle.gif" alt="OxMQ Job Lifecycle Animation" width="100%">
</p>

```mermaid
stateDiagram-v2
    [*] --> WAITING: addStandardJob-9.lua [delay == 0]
    [*] --> DELAYED: addDelayedJob-6.lua [delay > 0]
    [*] --> WAITING_CHILDREN: addParentJob-6.lua [parent node]

    DELAYED --> WAITING: Maturity timestamp reached (now >= score)
    WAITING_CHILDREN --> WAITING: All children completed (unresolved == 0)

    WAITING --> ACTIVE: moveToActive-11.lua (Worker acquires job & lock)
    
    state ACTIVE {
        [*] --> Executing
        Executing --> ProgressUpdated: updateProgress-3.lua
        ProgressUpdated --> Executing
        Executing --> LockExtended: extendLock-2.lua Heartbeat
        LockExtended --> Executing
    }

    ACTIVE --> COMPLETED: Success (moveToFinished-14.lua)
    ACTIVE --> DELAYED: Failed & retries remain (retryJob-11.lua with backoff)
    ACTIVE --> FAILED: Failed & max attempts exhausted
    ACTIVE --> WAITING: Lock expired (moveStalledJobsToWait-9.lua recovers job)

    FAILED --> WAITING: Explicit retry (retryJob-11.lua)
    
    COMPLETED --> [*]: TTL expired / cleanJobsInSet-3.lua
    FAILED --> [*]: TTL expired / cleanJobsInSet-3.lua
```

---

## 3. High-Throughput Batch Dequeue Architecture

For high-volume data pipelines (e.g. audit logs, clickstream events, metrics ingestion into ClickHouse, Elasticsearch, or PostgreSQL), OxMQ provides **Batch Dequeue**:

```mermaid
sequenceDiagram
    autonumber
    participant Redis as Redis Storage
    participant BatchPoller as OxmqBatchWorker
    participant Loom as VirtualThreadPerTaskExecutor
    participant TargetDB as ClickHouse / Elasticsearch / Postgres

    BatchPoller->>Redis: moveToActiveBatch.lua (Pop up to 100 jobs atomically)
    Redis-->>BatchPoller: List<Job<T>> [100 items]
    BatchPoller->>Loom: submit(() -> processor.process(batch))
    activate Loom
    Loom->>TargetDB: 1 Bulk INSERT / _bulk API call (100 items)
    TargetDB-->>Loom: Batch Write Confirmed (200 OK)
    Loom->>Redis: moveToFinishedBatch.lua (Atomically complete all 100 jobs)
    deactivate Loom
```

---

## 4. Virtual Thread Concurrency Model (with Java 17 Graceful Fallback)

Unlike traditional thread pools that exhaust operating system carrier threads when tasks perform blocking I/O (HTTP calls, DB queries, LLM inferences), OxMQ leverages Java 21 **Virtual Threads (Project Loom)** on JVM 21+, with automatic, zero-config fallback to platform thread pools on **Java 17 LTS** via [`DispatcherExecutorFactory`](/guide/virtual-threads):

```mermaid
sequenceDiagram
    autonumber
    participant Redis as Redis Cluster
    participant Poller as OxMQ Poller Thread
    participant Loom as VirtualThreadPerTaskExecutor
    participant Worker as Virtual Thread #N
    participant IO as External I/O (HTTP/DB/LLM)

    loop Continuous Non-Blocking Polling
        Poller->>Redis: moveToActive.lua (Atomic pop & set lock)
        Redis-->>Poller: Job Payload [ID: 1042]
        Poller->>Loom: submit(JobTask)
        Note over Poller: Immediately ready for next job (Zero blocking)
    end

    activate Worker
    Loom->>Worker: Spawn Virtual Thread
    Worker->>Worker: job.updateProgress(25%)
    Worker->>IO: Dispatch HTTP Request / LLM Call
    Note over Worker: Carrier thread unmounted during blocking I/O!
    IO-->>Worker: HTTP 200 OK Response
    Worker->>Redis: moveToFinished.lua (Save return value & release lock)
    Worker->>Redis: Record Micrometer Metrics (Duration & Status)
    deactivate Worker
```

---

## 5. Parent-Child DAG Workflow Engine (`FlowProducer`)

OxMQ supports complex task trees where parent tasks dynamically activate and consume the results of their children:

<p align="center">
  <img src="/assets/oxmq-dag-workflow.gif" alt="OxMQ Parent-Child DAG Workflow Resolution" width="100%">
</p>

```mermaid
graph TD
    Parent["Parent Job: Video Assembly<br/>(State: WAITING_CHILDREN, unresolvedChildren = 3)"]
    
    Child1["Child 1: Transcode 1080p<br/>(State: ACTIVE)"]
    Child2["Child 2: Transcode 720p<br/>(State: ACTIVE)"]
    Child3["Child 3: Transcode 480p<br/>(State: ACTIVE)"]

    Child1 -->|On Finish: HSET childrenValues & Decr counter| Parent
    Child2 -->|On Finish: HSET childrenValues & Decr counter| Parent
    Child3 -->|On Finish: Counter reaches 0 -> Move to WAITING| Parent

    style Parent fill:#2d3748,stroke:#4a5568,stroke-width:2px,color:#fff
    style Child1 fill:#2b6cb0,stroke:#3182ce,stroke-width:2px,color:#fff
    style Child2 fill:#2b6cb0,stroke:#3182ce,stroke-width:2px,color:#fff
    style Child3 fill:#2b6cb0,stroke:#3182ce,stroke-width:2px,color:#fff
```

---

## 6. Native Performance Telemetry Pipeline

```mermaid
graph LR
    subgraph OxmqCore["OxMQ Execution Hooks"]
        EnqHook["Job Enqueued"]
        WaitHook["Job Dequeued (Wait Latency)"]
        ExecHook["Job Executed (Duration & SLA)"]
        FailHook["Job Failed / Retried"]
    end

    subgraph MicrometerEngine["OxmqMetrics (Micrometer)"]
        CounterEnq["oxmq.jobs.enqueued (Counter)"]
        CounterComp["oxmq.jobs.completed (Counter)"]
        CounterFail["oxmq.jobs.failed (Counter)"]
        TimerDuration["oxmq.job.duration (p50, p95, p99 Timer)"]
        TimerWait["oxmq.job.wait_time (Timer)"]
        Gauges["oxmq.jobs.active / waiting / delayed (Gauges)"]
    end

    subgraph Observability["Dashboards & Sinks"]
        Prometheus["Prometheus / OpenTelemetry"]
        Grafana["Grafana Dashboards"]
        Actuator["Spring Boot /actuator/metrics"]
    end

    EnqHook --> CounterEnq
    WaitHook --> TimerWait
    ExecHook --> TimerDuration
    ExecHook --> CounterComp
    FailHook --> CounterFail

    MicrometerEngine --> Prometheus
    MicrometerEngine --> Actuator
    Prometheus --> Grafana
```

---

## 7. Official BullMQ Lua Script Engine & Key Topology

OxMQ directly incorporates the complete suite of **49 official standalone Lua scripts from BullMQ v6**. By executing the exact same Lua state transitions as BullMQ, OxMQ guarantees 100% wire and functional compatibility:

### Key Topology (`QueueKeys`)
| Key | Redis Type | Purpose |
| :--- | :--- | :--- |
| `bull:<queue>:wait` | LIST / STREAM | Pending jobs waiting to be claimed by workers |
| `bull:<queue>:active` | LIST | Currently executing jobs locked by workers |
| `bull:<queue>:delayed` | ZSET | Jobs scheduled for future timestamps (`score = timestamp`) |
| `bull:<queue>:waiting-children` | ZSET | Parent jobs waiting for dependencies to complete |
| `bull:<queue>:completed` | ZSET | Successfully finished jobs |
| `bull:<queue>:failed` | ZSET | Failed jobs that have exhausted all retries |
| `bull:<queue>:<jobId>` | HASH | Job metadata, payload, opts, stacktrace, progress |
| `bull:<queue>:<jobId>:lock` | STRING | Worker ownership token with lock lease TTL |
| `bull:<queue>:events` | STREAM / PUBSUB | Real-time state transition events |
| `bull:<queue>:dn:<dedupId>` | STRING | `keepLastIfActive` deduplication buffer storing latest payload |
| `bull:<queue>:pc` | ZSET | Priority counter set for prioritized jobs |

### BullMQ v6 Architectural Enhancements in OxMQ
1. **Priority-Preserving Retries (`reprocessJob-9.lua`)**: Re-queuing failed or completed jobs checks if `priority > 0` and atomically adds the job back to the priority sorted set (`KEYS[8] prioritized`, `KEYS[9] pc`), ensuring high-priority jobs jump ahead upon dead-letter replay.
2. **DAG Child Dependency Re-attachment (`moveJobsToWait-8.lua`)**: In bulk retries, the script detects parent linkages (`parentKey`) and atomically restores retried child jobs to `parentKey:dependencies`, preventing premature parent job resolution.
3. **Zero-Attempt Worker Postponement (`moveToDelayed-11.lua`)**: Accepts `skipAttempt: true` (ARGV[6]), enabling workers to defer execution via `DelayedException` without burning attempt budgets.
4. **Deduplication Buffer Re-queueing (`moveToFinished-14.lua`)**: Automatically checks the `dn:<dedupId>` key and re-dispatches the last deduplicated job upon active task completion.

### MessagePack Binary Protocol (`BullMsgPack`)
BullMQ scripts unpack complex arguments (`opts`, `jobArgs`) using `cmsgpack.unpack(ARGV[i])`. OxMQ encodes these options into binary MessagePack format using Jackson's MessagePack format and maps key names using BullMQ's option compression table (`fpof` for `failParentOnFailure`, `cpof` for `continueParentOnFailure`, `idof` for `ignoreDependencyOnFailure`, etc.).

---

## 8. Memory Optimization & Resource Efficiency Architecture

In high-throughput distributed systems processing tens of thousands of jobs per second, unbounded memory allocations can degrade GC latency or exhaust Redis capacity:

```mermaid
flowchart LR
    subgraph Consumer["☕ JVM Consumer Optimizations"]
        Intern["Canonical Key Interning<br/>(Zero string allocations on hot path)"]
        Lazy["Lazy Child Resolution<br/>(0 roundtrips for standalone jobs)"]
        Guard["Stack Trace Guardrail<br/>(8KB truncation ceiling)"]
        Mux["Lettuce Multiplexing<br/>(Watchdogs share binary connection)"]
    end
    subgraph Storage["⚡ Redis Storage Optimizations"]
        Trim["Stream Event Trimming<br/>(XADD MAXLEN ~ N)"]
        AutoRetention["Default Job Options<br/>(Auto removeOnComplete / Fail)"]
        Compaction["ZSET Compaction<br/>(Automatic hash eviction)"]
    end
    Consumer <--> Storage
```

1. **Zero-Allocation Poller Hot-Path**:
   - Interns canonical BullMQ hash field keys (`id`, `name`, `data`, `opts`, etc.) and pre-sizes field map capacities to prevent map rehashes and Young Gen garbage collector churn.
2. **Lazy Parent-Child Result Supplier**:
   - `Job<T>` defers fetching child job return values via a `Supplier<Map<String, Object>>`. Standalone jobs (99.9% of traffic) execute **0 Redis network calls** for `:processed` hashes.
3. **Lettuce Connection Multiplexing**:
   - `LockExtender` (heartbeats) and `StalledJobSentinel` (recovery) multiplex over the shared thread-safe binary connection, slashing background TCP sockets by 66%. Dedicated blocking sockets (`bpop`, `xread`) are guarded with deterministic `try ... finally { conn.close(); }` closures.
4. **Stack Trace Truncation Guardrail**:
   - Limits exception failure reasons to `8,192` characters (8 KB) with an explicit marker to protect Redis RAM and consumer heap during cascading outage loops.
5. **Global Retention Inheritance & Stream Trimming**:
   - Queues and Spring Boot starters automatically apply `removeOnComplete` and `removeOnFail` defaults without requiring repetitive configuration on every `queue.add(...)` call.
   - Bounded Redis Streams (`maxLenEvents`) ensure `events` streams never grow unbounded under sustained load.

*For complete implementation details, benchmarking metrics, and production tuning parameters, see the [Memory Optimization & Resource Efficiency Guide](/operations/memory-optimization).*

---

## 🙏 Attribution

OxMQ builds upon the foundational queue architecture developed by the open-source **[BullMQ](https://github.com/taskforcesh/bullmq)** community. The official BullMQ Lua scripts are included under the permissive MIT license in [`BULLMQ_ATTRIBUTION.md`](https://github.com/gaurav10610/oxmq/blob/main/oxmq-core/src/main/resources/lua/BULLMQ_ATTRIBUTION.md).

---

## 👤 Architect & Author

**Gaurav Kumar Yadav**
* 💼 **LinkedIn:** [linkedin.com/in/gaurav-kumar-yadav-6125817a](https://www.linkedin.com/in/gaurav-kumar-yadav-6125817a/)
* 🐙 **GitHub:** [@gaurav10610](https://github.com/gaurav10610)

