# 🏛️ OxMQ Architecture & Internals

This document details the architectural design, Redis data structures, concurrency model, DAG execution engine, and performance telemetry pipeline of **OxMQ**.

---

## 1. High-Level System Architecture

OxMQ is engineered around 4 core principles:
1. **Atomic Redis State Transitions:** All state transitions (`WAITING` $\rightarrow$ `ACTIVE` $\rightarrow$ `COMPLETED` / `FAILED` / `DELAYED`) are executed via single-roundtrip Lua scripts.
2. **Virtual Thread Native (Project Loom):** Every task execution runs in an ultra-lightweight Java 21 Virtual Thread, enabling 1,000+ to 10,000+ concurrent I/O-bound workers with near-zero memory overhead.
3. **BullMQ v5 Wire-Compatibility:** Standard BullMQ Redis key hierarchies enable seamless polyglot interoperability with Node.js/Python workers and instant compatibility with **Bull-Board UI**.
4. **Native Performance Instrumentation:** Microsecond-resolution Micrometer timers, gauges, and counters built into the core path.

```mermaid
graph TB
    subgraph AppLayer["Application Layer"]
        Producer["OxmqQueue&lt;T&gt;<br/>(Producer)"]
        Flow["FlowProducer<br/>(DAG Workflows)"]
        SpringWorker["@OxmqListener<br/>(Spring Boot 3)"]
        CoreWorker["OxmqWorker&lt;T&gt;<br/>(Standalone Java 21)"]
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

    LoomDispatcher --> LuaManager
    LockWatchdog --> LuaManager
    RateLimit --> LuaManager

    LuaManager --> RedisStorage
    MetricsEngine --> Prometheus
    EventsPubSub --> BullBoardUI
    JobHashes --> BullBoardUI
```

---

## 2. Job Lifecycle State Machine

Jobs transition through a strictly enforced, atomic state machine managed by Redis Lua scripts:

```mermaid
stateDiagram-v2
    [*] --> WAITING: queue.add() [delay == 0]
    [*] --> DELAYED: queue.add() [delay > 0]
    [*] --> WAITING_CHILDREN: flowProducer.add() [parent node]

    DELAYED --> WAITING: Maturity timestamp reached (now >= score)
    WAITING_CHILDREN --> WAITING: All children completed (unresolved == 0)

    WAITING --> ACTIVE: moveToActive.lua (Worker acquires job & lock)
    
    state ACTIVE {
        [*] --> Executing
        Executing --> ProgressUpdated: job.updateProgress()
        ProgressUpdated --> Executing
        Executing --> LockExtended: LockExtender Heartbeat
        LockExtended --> Executing
    }

    ACTIVE --> COMPLETED: Success (moveToFinished.lua)
    ACTIVE --> DELAYED: Failed & retries remain (retryJob.lua with backoff)
    ACTIVE --> FAILED: Failed & max attempts exhausted
    ACTIVE --> WAITING: Lock expired (StalledJobSentinel recovers job)

    FAILED --> WAITING: Explicit retry (queue.retryJob)
    
    COMPLETED --> [*]: TTL expired / cleanQueue
    FAILED --> [*]: TTL expired / cleanQueue
```

---

## 3. Concurrency & Virtual Thread Execution Model

Unlike traditional thread pools that exhaust operating system carrier threads when tasks perform blocking I/O (HTTP calls, DB queries, LLM inferences), OxMQ leverages Java 21 **Virtual Threads (Project Loom)**:

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

## 4. Parent-Child DAG Workflow Engine (`FlowProducer`)

OxMQ supports complex task trees where parent tasks dynamically activate and consume the results of their children:

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

### Execution Flow:
1. `FlowProducer.add(tree)` writes child jobs to `bull:<q>:wait` and the parent job to `bull:<q>:<parentId>` with state `WAITING_CHILDREN` and `unresolvedChildrenCount = N`.
2. When each child completes, `moveToFinished.lua` stores the child's return value into `bull:<parentQueue>:<parentId>:childrenValues` and atomically decrements the unresolved count.
3. When `unresolvedChildrenCount` reaches `0`, Redis moves the parent job to `bull:<parentQueue>:wait` for immediate execution.

---

## 5. Redis Data Structure Layout (BullMQ Wire-Compatibility)

For a queue named `notifications`, OxMQ manages the following Redis keys:

| Key Pattern | Redis Type | Description |
| :--- | :--- | :--- |
| `bull:notifications:id` | `String` | Monotonically increasing atomic ID generator. |
| `bull:notifications:wait` | `List` | FIFO list of job IDs ready for immediate consumption. |
| `bull:notifications:active` | `List` | Job IDs currently being processed by active workers. |
| `bull:notifications:delayed` | `Sorted Set` | Delayed job IDs sorted by millisecond execution timestamp. |
| `bull:notifications:completed` | `Sorted Set / List` | Completed job IDs sorted by completion timestamp. |
| `bull:notifications:failed` | `Sorted Set / List` | Failed job IDs with error reasons. |
| `bull:notifications:stalled` | `Sorted Set` | Watchdog lock verification keys. |
| `bull:notifications:meta` | `Hash` | Queue metadata (paused state, rate limit settings). |
| `bull:notifications:limiter` | `Sorted Set` | Sliding window timestamps for rate limiting. |
| `bull:notifications:<jobId>` | `Hash` | Job fields: `name`, `data`, `opts`, `progress`, `returnvalue`, `failedReason`. |
| `bull:notifications:<jobId>:lock` | `String` | Ephemeral worker ownership lock with TTL (`lockDuration`). |
| `bull:notifications:<jobId>:logs` | `List` | Appended log messages for visual debugging. |

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
