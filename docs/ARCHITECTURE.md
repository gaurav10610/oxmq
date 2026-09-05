# OxMQ Architecture & Internals

This document details the architectural design, Redis data structures, concurrency model, DAG execution engine, and performance telemetry pipeline of **OxMQ**.

---

## 1. High-Level System Architecture

OxMQ is engineered around 4 core principles:
1. **Atomic Redis State Transitions:** All state transitions (`WAITING` $\rightarrow$ `ACTIVE` $\rightarrow$ `COMPLETED` / `FAILED` / `DELAYED`) are performed via single-roundtrip Lua scripts.
2. **Virtual Thread Native (Project Loom):** Every task execution runs in an ultra-lightweight Java 21 Virtual Thread, enabling 1,000+ concurrent I/O-bound workers without thread pool starvation.
3. **BullMQ v5 Wire-Compatibility:** Uses standard BullMQ Redis key hierarchies, allowing seamless polyglot interoperability with Node.js/Python workers and the **Bull-Board** UI.
4. **Native Performance Instrumentation:** Microsecond-resolution Micrometer timers, gauges, and counters built into the core path.

```
                  +----------------------------------------------+
                  |               User Application               |
                  |     OxmqQueue<T>      /     @OxmqListener    |
                  +----------------------------------------------+
                                         │
                   ┌─────────────────────┴─────────────────────┐
                   ▼                                           ▼
      +─────────────────────────+                 +─────────────────────────+
      |       OxmqQueue         |                 |       OxmqWorker        |
      |   - add / addBulk       |                 |   - VirtualThreadPerTask|
      |   - FlowProducer        |                 |   - LockExtender        |
      |   - pause / resume      |                 |   - StalledJobSentinel  |
      +─────────────────────────+                 +─────────────────────────+
                   │                                           │
                   └─────────────────────┬─────────────────────┘
                                         ▼
                  +----------------------------------------------+
                  |              LuaScriptManager                |
                  |   - SHA-1 Caching & EVALSHA Execution        |
                  |   - Automatic EVAL fallback on NOSCRIPT      |
                  +----------------------------------------------+
                                         │
                                         ▼
                  +----------------------------------------------+
                  |               Redis Cluster / DB             |
                  |   - bull:<queue>:wait      (List)            |
                  |   - bull:<queue>:active    (List)            |
                  |   - bull:<queue>:delayed   (ZSet)            |
                  |   - bull:<queue>:<id>      (Hash)            |
                  |   - bull:<queue>:stalled   (ZSet)            |
                  |   - bull:<queue>:limiter   (ZSet/String)     |
                  +----------------------------------------------+
```

---

## 2. Redis Data Structure Layout (BullMQ Wire Compatibility)

For a queue named `notifications`, OxMQ creates the following Redis keys:

| Key Pattern | Redis Type | Description |
| :--- | :--- | :--- |
| `bull:notifications:id` | `String` | Monotonically increasing auto-increment counter for job IDs. |
| `bull:notifications:wait` | `List` | FIFO list of job IDs ready for immediate consumption. |
| `bull:notifications:active` | `List` | List of job IDs currently being processed by active workers. |
| `bull:notifications:delayed` | `Sorted Set` | Sorted set of delayed job IDs, where `score` is the millisecond timestamp when the job becomes ready. |
| `bull:notifications:completed` | `Sorted Set / List` | IDs of completed jobs for history and cleanup TTL. |
| `bull:notifications:failed` | `Sorted Set / List` | IDs of failed jobs with stack traces. |
| `bull:notifications:stalled` | `Sorted Set` | Heartbeat tracking timestamps for detecting hung workers. |
| `bull:notifications:meta` | `Hash` | Queue metadata (paused state, rate limit settings). |
| `bull:notifications:limiter` | `Hash / String` | Token bucket / sliding window state for rate limiting. |
| `bull:notifications:<jobId>` | `Hash` | Full job record: `name`, `data`, `opts`, `progress`, `returnvalue`, `failedReason`, timestamps. |
| `bull:notifications:<jobId>:logs` | `List` | Appended log messages for visual debugging. |

---

## 3. Atomic Lua Script State Machine

State transitions in OxMQ never suffer from race conditions or split-brain states because they are executed inside atomic Lua scripts:

### A. `addJob.lua`
1. Resolves or generates the unique `jobId`.
2. Checks for deduplication if custom `jobId` exists.
3. Writes the job Hash (`bull:<q>:<id>`).
4. If `delay > 0`, adds `jobId` to `bull:<q>:delayed` with score `now + delay`.
5. If `delay == 0`, pushes `jobId` to `bull:<q>:wait` and publishes an event on `bull:<q>:events`.

### B. `moveToActive.lua`
1. Checks if the queue is paused or rate-limited.
2. Promotes any matured jobs from `bull:<q>:delayed` to `bull:<q>:wait`.
3. Atomically pops from `bull:<q>:wait` and pushes to `bull:<q>:active`.
4. Sets the worker lock key `bull:<q>:<jobId>:lock` with TTL = `lockDuration`.
5. Updates job hash `processedOn = now`, `attemptsMade = attemptsMade + 1`.

### C. `moveToFinished.lua`
1. Releases the worker lock key.
2. Removes `jobId` from `bull:<q>:active`.
3. If successful: saves `returnvalue`, adds to `bull:<q>:completed`, notifies parent job if part of a DAG.
4. If failed and `attemptsMade < maxAttempts`: calculates backoff delay and moves to `bull:<q>:delayed`.
5. If failed and retries exhausted: saves `failedReason` and adds to `bull:<q>:failed`.

---

## 4. Virtual Thread Concurrency Model

OxMQ is engineered from the ground up for Java 21 Virtual Threads:

```
[Redis Stream / Wait List]
           │
           │ (Atomic moveToActive)
           ▼
[Worker Poller Loop]
           │
           │ submits Runnable task
           ▼
[VirtualThreadPerTaskExecutor]
   ├── [ Virtual Thread #1 ] ──> HTTP Webhook (Blocking I/O - unmounts carrier thread)
   ├── [ Virtual Thread #2 ] ──> Database Query (Blocking I/O - unmounts carrier thread)
   ├── [ Virtual Thread #3 ] ──> LLM API Call (Blocking I/O - unmounts carrier thread)
   └── [ Virtual Thread #N ] ──> S3 Upload (Blocking I/O - unmounts carrier thread)
```

- When a Virtual Thread blocks on network/database I/O, Java 21 unmounts it from the underlying carrier (OS) thread.
- A single node can comfortably execute **1,000 to 10,000 concurrent jobs** with minimal heap overhead (< 50MB).
- Configurable concurrency semaphore ensures rate limits and system resource caps are strictly respected.

---

## 5. Parent-Child DAG Workflow Engine (`FlowProducer`)

OxMQ allows defining complex task dependency trees:

```
                  ┌─────────────────┐
                  |   Parent Job    |
                  | (Video Encoder) |
                  └────────┬────────┘
                           │ (Waits for all children)
        ┌──────────────────┼──────────────────┐
        ▼                  ▼                  ▼
┌───────────────┐  ┌───────────────┐  ┌───────────────┐
| Chunk 1 (1080p)|  | Chunk 2 (1080p)|  | Chunk 3 (1080p)|
└───────────────┘  └───────────────┘  └───────────────┘
```

1. `FlowProducer.add(FlowJob tree)` pushes child jobs to `bull:<q>:wait` and the parent job with state `WAITING_CHILDREN` and an unresolved child counter.
2. As each child completes in `moveToFinished.lua`, Redis atomically decrements the parent's child counter and stores the child's return value in `bull:<q>:<parentId>:childrenValues`.
3. When the counter reaches `0`, Redis automatically moves the parent job to `bull:<q>:wait`.

---

## 6. Native Performance Telemetry Pipeline

OxMQ embeds a native `OxmqMetrics` engine powered by Micrometer:

* **Counters:**
  - `oxmq.jobs.enqueued`: Total jobs submitted (tagged by `queue`).
  - `oxmq.jobs.completed`: Total jobs successfully finished (tagged by `queue`).
  - `oxmq.jobs.failed`: Total jobs failed (tagged by `queue`, `exception`).
  - `oxmq.jobs.retried`: Total retry attempts scheduled.
  - `oxmq.jobs.stalled`: Stalled jobs recovered by watchdog.
* **Gauges:**
  - `oxmq.jobs.active`: Currently executing jobs.
  - `oxmq.jobs.waiting`: Jobs pending in wait list.
  - `oxmq.jobs.delayed`: Jobs waiting on scheduled delay.
* **Timers:**
  - `oxmq.job.duration`: Execution time histogram with p50, p95, p99 percentiles.
  - `oxmq.job.wait_time`: Queue residence time before pickup.
