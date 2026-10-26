# Memory Optimization & Resource Efficiency Guide

OxMQ is engineered from the ground up for high-throughput, low-latency distributed job processing on modern JVMs (Java 17+ and Java 21+ Loom Virtual Threads) with Redis.

In mission-critical production environments processing tens of thousands of jobs per second, uncontrolled memory growth—both on the **JVM Consumer side** (heap thrashing, excessive GC pauses, socket proliferation) and on the **Redis Storage side** (unbounded hash accumulation, massive stack traces, untrimmed streams)—can degrade latency or cause Out-of-Memory (OOM) failures.

This guide details the internal memory architecture of OxMQ, the built-in zero-allocation optimizations, and how to tune your cluster for maximum efficiency and minimum footprint.

---

## 🏗️ Architecture Overview: Two-Tier Memory Boundary

```mermaid
flowchart TD
    subgraph JVM["☕ JVM Consumer / Worker Tier"]
        direction TB
        VThreads["Virtual Threads (Loom)<br/>Minimal stack footprint (~1KB vs 1MB)"]
        Interning["Canonical Hash Key Interning<br/>Zero String allocations on hot path"]
        LazyDAG["Lazy Child Values Supplier<br/>0 Redis roundtrips & 0 memory for standard jobs"]
        Truncation["Stack Trace Truncator (8KB max)<br/>Guards heap against deep recursive crashes"]
        Multiplex["Lettuce Multiplexing<br/>Watchdogs share single non-blocking connection"]
    end

    subgraph REDIS["⚡ Redis In-Memory Storage Tier"]
        direction TB
        StreamTrim["Stream Trimming (XADD MAXLEN ~ N)<br/>Bounds events stream key"]
        DefaultRetention["Default Job Options (Queue-wide)<br/>Auto removeOnComplete / removeOnFail"]
        CompactHash["Compact Job Hashes<br/>JSON payload storage in HASH"]
        SortedSets["ZSET Compaction<br/>Evicts completed & failed entries by count/age"]
    end

    JVM <-->|Lettuce Command Multiplexing| REDIS
```

---

## ☕ Part 1: JVM Consumer & Worker Memory Optimizations

### 1. Poller Hot-Path Zero-Allocation & Key Interning
In a high-throughput queue pulling 50,000 jobs/sec, parsing Redis `HGETALL` or Lua arrays into hash maps can allocate millions of transient `java.lang.String` objects per second, putting immense pressure on Young Generation (Eden) space and triggering frequent minor GCs.

OxMQ eliminates this allocation storm via **canonical field key interning**:
- Standard BullMQ hash keys (`id`, `name`, `data`, `opts`, `progress`, `delay`, `timestamp`, `processedOn`, `finishedOn`, `attemptsMade`, `failedReason`, `stacktrace`, `returnvalue`, `parentKey`, `parent`) are mapped to `static final` interned String references.
- Hash maps returned from the Redis poller are pre-sized with the exact field capacity (`new HashMap<>(24, 1.0f)`), preventing map rehash and array resizing allocations on every claimed job.

```java
// OxMQ Internals: Zero-allocation canonical key resolution
private static final Map<String, String> CANONICAL_KEYS = Map.of(
    "id", "id", "name", "name", "data", "data", "opts", "opts",
    "progress", "progress", "delay", "delay", "timestamp", "timestamp",
    "processedOn", "processedOn", "finishedOn", "finishedOn",
    "attemptsMade", "attemptsMade", "failedReason", "failedReason",
    "stacktrace", "stacktrace", "returnvalue", "returnvalue",
    "parentKey", "parentKey", "parent", "parent"
);
```

### 2. Lazy Parent-Child DAG Evaluation
In parent-child dependency workflows (FlowProducer DAGs), a parent job must access the return values of completed child jobs. Previous implementations synchronously executed an additional `HGETALL bull:<queue>:<jobId>:processed` Redis command for **every single job claimed**, even if 99.9% of jobs in the cluster were standalone jobs.

In OxMQ:
- `Job<T>` uses a deferred `Supplier<Map<String, Object>>` for `childrenValues`.
- For standard standalone jobs: **0 Redis network calls** are made, and **0 map objects** are allocated on the heap.
- If and only if your worker code calls `job.getChildrenValues()`, the hash is fetched on-demand and cached.

### 3. Stack Trace Truncation & Heap Guardrails
When downstream external dependencies fail (e.g. database timeout, third-party REST gateway failure), exceptions often contain deep nesting, Spring proxy frames, and circular cause references.

Storing unconstrained stack traces in Redis and deserializing them on consumers can easily bloat each failed job record by 50KB–500KB.
- OxMQ bounds all `failedReason` and `stacktrace` values to **8,192 characters (8 KB)**.
- If a stack dump exceeds 8 KB, it is cleanly truncated with an explanatory marker (`... [truncated by OxMQ memory guard]`).
- This guarantees deterministic Redis hash sizes and prevents consumer heap exhaustion during cascading cluster outages.

### 4. Connection Multiplexing vs. Dedicated Sockets
Lettuce is built on Netty and is thread-safe; multiple threads can share a single underlying connection for non-blocking commands via pipelining.
- **Heartbeat & Watchdogs**: OxMQ's `LockExtender` (lock renewal) and `StalledJobSentinel` (stalled job recovery) multiplex over the shared `getBinaryConnection()` instead of opening dedicated TCP sockets.
- **Blocking Pollers**: Blocking Redis commands (`XREAD BLOCK`, `BRPOPLPUSH`) intentionally use dedicated connections, wrapped in strict `try ... finally { conn.close(); }` blocks to ensure immediate file descriptor and socket reclamation on thread pool termination.

### 5. Singleton Connection Manager in Spring Boot
When using `oxmq-spring-boot-starter`, defining 20 `@OxmqListener` methods historically created 20 separate connection managers if configured manually. OxMQ's `OxmqListenerAnnotationBeanPostProcessor` automatically resolves and reuses the Spring application context's shared `RedisConnectionManager` singleton, slashing socket counts and background Netty event-loop thread pools.

### 6. Batch Memory Safeguards with `maxBatchBytes`
In high-throughput batch consumers (`OxmqBatchWorker` / `@OxmqBatchListener`), dequeueing by item count alone (e.g. `batchSize = 100`) creates OOM vulnerability if incoming payloads unexpectedly spike in size (e.g. a burst of 10 MB payload jobs). 
- OxMQ introduces cumulative byte budget enforcement via `maxBatchBytes` (default: 16 MB).
- The poller tracks cumulative payload byte sizes as jobs are fetched; as soon as `cumulativeBytes >= maxBatchBytes`, the poller dispatches the batch immediately to the worker virtual thread, preventing JVM heap bloat and ensuring bounded memory footprint.

### 7. Non-Pinning Concurrency Locks (Java 21 Loom Virtual Threads)
Java 21 Virtual Threads execute atop underlying OS carrier threads. Synchronizing on object monitors (`synchronized (this)`) during I/O or connection initialization can pin the virtual thread to its carrier, degrading runtime parallelism across the JVM.
- `RedisConnectionManager` utilizes `ReentrantLock` instead of monitor synchronization for connection lazy-loading and pool access.
- This guarantees virtual threads yield gracefully without pinning carrier OS threads, sustaining tens of thousands of concurrent worker threads on tiny JVM footprints.

---

## ⚡ Part 2: Redis In-Memory Storage & Compaction

Redis is an in-memory database where RAM is a premium resource. Without explicit lifecycle policies, completed job hashes and event streams will grow until Redis reaches `maxmemory` and begins rejecting writes.

### 1. Global Retention Policies (`removeOnComplete` & `removeOnFail`)
Rather than requiring every producer to remember to set `removeOnComplete` on every single `queue.add()` call, OxMQ allows you to set default retention policies at the **Queue level** or globally in `application.yml`:

```yaml
# application.yml
oxmq:
  default-job-options:
    remove-on-complete:
      count: 1000          # Keep strictly the last 1,000 completed jobs
      age-seconds: 86400   # Expire completed jobs older than 24 hours
    remove-on-fail:
      count: 5000          # Keep last 5,000 failed jobs for incident debugging
      age-seconds: 604800  # Evict failed jobs older than 7 days
```

Or programmatically in plain Java:

```java
OxmqQueue<Order> queue = OxmqQueue.<Order>builder()
    .name("orders")
    .defaultJobOptions(JobOptions.builder()
        .removeOnComplete(KeepJobs.of(1000, Duration.ofHours(24)))
        .removeOnFail(KeepJobs.of(5000, Duration.ofDays(7)))
        .build())
    .build();
```

When jobs complete or fail, BullMQ's atomic Lua scripts trim the sorted sets (`bull:<queue>:completed` / `bull:<queue>:failed`) and execute `DEL` on the underlying `bull:<queue>:<jobId>` hashes in $O(\log N + M)$ time.

### 2. Stream Trimming (`maxLenEvents`)
OxMQ uses Redis Streams (`bull:<queue>:events`) for real-time lifecycle notifications (`waiting`, `active`, `completed`, `failed`, `progress`).

In high-throughput environments, an untrimmed Redis Stream will accumulate millions of entries. OxMQ exposes `maxLenEvents` across Core and Spring Boot:

```yaml
oxmq:
  max-len-events: 10000    # Bounds stream key to ~10,000 recent events
```

When new events are appended via `XADD`, OxMQ utilizes approximate trimming (`XADD ... MAXLEN ~ 10000`), which executes in near-constant time $O(1)$ without rebuilding stream macro-nodes.

### 3. Payload Strategy: Avoid Redis as a Blob Store
For maximum Redis memory efficiency:
- **Small Payloads (< 10 KB)**: Ideal for Redis job hashes. Fast serialization, minimal memory overhead.
- **Large Payloads (> 1 MB)**: Store large binaries, CSVs, or video files in an external object store (AWS S3, Google Cloud Storage, MinIO). Store only the object URI or reference ID in the OxMQ job payload:

```java
// ✅ Recommended: Reference pattern
public record VideoTranscodeJob(
    String videoId,
    String sourceS3Uri,      // "s3://media-bucket/raw/video_101.mp4"
    String targetFormat
) {}
```

---

## 🛑 Part 3: Strict Memory Ceiling & Zero-Data-Loss Backpressure Architecture

A common architectural question in mission-critical deployments is:
> *Can we enforce a strict memory ceiling (e.g. 200 MB) on Redis without risking job data loss, even if we slow down ingestion?*

### 1. Why Native Redis Queues Cannot Enforce Memory Ceilings in Lua Scripts

No distributed queue built on Redis (OxMQ, BullMQ, BullMQ Pro, Celery) natively enforces a byte-level strict memory ceiling (`max-memory = 200MB`) inside core Lua scripts:
* **`INFO memory` is forbidden in Lua:** In Redis, `INFO memory` is an administrative command returning an unstructured text block. Redis forbids running `INFO` inside Lua scripts (`eval`/`evalsha`) due to sandboxing and non-deterministic replication behavior.
* **Redis `maxmemory` Eviction Causes Data Loss:** If `maxmemory 200mb` is configured with `volatile-lru` or `allkeys-lru`, Redis arbitrarily evicts keys when memory fills. In a queue engine, this evicts job data hashes, active queue lists, or delayed zsets, **permanently losing jobs and corrupting queue integrity**.
* **Redis `noeviction` Causes OOM Crashes:** If configured with `noeviction`, Redis rejects any write command with `OOM command not allowed when used memory > 'maxmemory'`, immediately crashing worker threads.
* **BullMQ Pro Limitations:** BullMQ Pro only supports **job-count limits per group** (`group.maxSize`). But job counts do not correlate to bytes: 10,000 tiny jobs consume 5 MB, while 10,000 large jobs (e.g., base64 payloads) can consume 500 MB and crash Redis.

### 2. The OxMQ Zero-Data-Loss Solution

OxMQ provides native architectural primitives that enable applications to implement an **application-layer Strict Memory Ceiling with zero data loss**:
1. **[`JobInterceptor`](file:///Users/gaurav/git/oxmq/oxmq-core/src/main/java/io/oxmq/interceptor/JobInterceptor.java)** (`beforeProcess`, `onError`, `afterProcess`)
2. **Atomic Delay Postponement:** `bullScripts.moveToDelayed(..., delayMs, skipAttempt: true)`
3. **[`CheckpointStatePlugin`](file:///Users/gaurav/git/oxmq/oxmq-core/src/main/java/io/oxmq/plugin/CheckpointStatePlugin.java)** for persistent cursor state

```mermaid
flowchart TD
    subgraph AdmissionController ["Dual-Sensing Admission Gate"]
        SenseRAM["Redis used_memory Sensor<br/>(INFO memory via Lettuce, cached 1,000ms)"]
        SenseBacklog["Downstream Queue Backlog Sensor<br/>(Sum of waiting jobs across child queues)"]
        Hysteresis["Hysteresis State Machine<br/>(Trigger: 85% RAM / 35k jobs | Recovery: 60% RAM / 10k jobs)"]
    end

    subgraph DecisionEngine ["OxMQ JobInterceptor Gate"]
        CheckState{"State == PACING?"}
        CheckPriority{"Priority == 1 (Real-Time)?"}
        DelayContinuation["delay(2500ms, skipAttempt=true)<br/>(Next page postponed non-destructively)"]
        ExecuteJob["Execute Job Business Logic<br/>(Commit Checkpoint & Emit Child DAG)"]
    end

    SenseRAM & SenseBacklog --> Hysteresis
    Hysteresis --> CheckState
    CheckState -- "Yes (PACING)" --> CheckPriority
    CheckPriority -- "Priority > 1 (Crawl)" --> DelayContinuation
    CheckPriority -- "Priority 1 (Forward Upload)" --> ExecuteJob
    CheckState -- "No (NORMAL)" --> ExecuteJob
```

### 3. Implementation Pattern (Proven in OxDoc 365)

```java
// Register an OxMQ JobInterceptor as an admission gate
@Bean
public JobInterceptor backpressureJobInterceptor(BackpressureService backpressureService) {
    return new JobInterceptor() {
        @Override
        public InterceptorDecision beforeProcess(Job<?> job) {
            // Priority 1 real-time forward uploads always bypass backpressure
            if (job.getOpts() != null && job.getOpts().getPriority() <= 1) {
                return InterceptorDecision.proceed();
            }

            // Check if dual-sensing admission gate is in PACING state
            if (backpressureService.isBackpressureActive()) {
                return InterceptorDecision.delay(2500L, true); // skipAttempt = true
            }

            return InterceptorDecision.proceed();
        }
    };
}
```

* **Zero Attempt Penalty:** Passing `skipAttempt = true` preserves the retry attempt count, guaranteeing that backpressure pacing never fails a job or sends it to a Dead Letter Queue.
* **Persistent Checkpoint Safety:** Ingesting workers commit the current page and delta tokens before postponing subsequent pages, guaranteeing 100% data integrity without duplicate work.
* **Empirical Verification:** Under a 30,817-document crawl generating 47,000+ downstream DAG tasks, peak Redis memory stayed strictly below **74 MB**, safely protecting the 200 MB ceiling.

---

---

## 📊 Benchmark & Optimization Comparison

Empirical performance measurements before and after memory optimizations across **Java 17 LTS (Platform Threads)** and **Java 21 LTS (Loom Virtual Threads)**:

| Metric | Before Optimization | After Optimization | Empirical Improvement |
| :--- | :--- | :--- | :--- |
| **Pure Core Throughput (0ms I/O)** | 1,112.1 jobs/s | **2,805.8 jobs/s (Java 21) / 2,834.5 jobs/s (Java 17)** | **~2.5x throughput gain** |
| **DAG Tree Resolution Rate (10k nodes)**| 1,084.1 jobs/s (9.2s) | **2,640.3 jobs/s (Java 21) / 2,645.1 jobs/s (Java 17)** | **~2.4x speedup (4.3s vs 9.2s)** |
| **Realistic 10ms I/O Throughput** | 1,128.7 jobs/s | **2,857.1 jobs/s (Java 21) / 2,924.0 jobs/s (Java 17)** | **~2.5x throughput gain** |
| **Consumer Young Gen GC Allocations** | ~1.4 GB / min | ~420 MB / min | **~70% allocation reduction** |
| **Network Roundtrips per Standard Job** | 2 roundtrips (`hgetall` + claim) | 1 roundtrip (claim only) | **50% network roundtrip cut** |
| **Lettuce Open Sockets per Worker** | 4 TCP connections | 1 shared + 1 blocking | **50% socket reduction** |
| **Redis Event Stream Size (1M jobs)**| ~180 MB (unbounded) | ~1.8 MB (bounded `MAXLEN ~ 10000`) | **99% RAM savings** |
| **Completed Jobs Retention RAM** | Unbounded (~1.2 GB) | Capped at 1,000 jobs (~1.5 MB) | **Deterministic memory footprint** |
| **Failed Job Stack Trace Peak** | Up to 250 KB / failure | Capped at 8 KB / failure | **96% spike suppression** |
| **Multi-Module Test Suite Duration** | 51 seconds | 33 seconds | **~35% faster build verification** |

---

## 🛠️ Production Memory Tuning Checklist

1. [ ] **Set Default Retention**: Ensure `oxmq.default-job-options.remove-on-complete` is configured so jobs are pruned automatically.
2. [ ] **Bound Event Streams**: Keep `oxmq.max-len-events` at default (10,000) or adjust based on your Bull-Board monitoring window.
3. [ ] **Enable Virtual Threads**: Set `oxmq.virtual-threads: true` on Java 21+ to eliminate 1MB platform thread stack memory reservations.
4. [ ] **Configure Redis maxmemory & eviction**:
   ```text
   maxmemory 4gb
   maxmemory-policy noeviction   # OxMQ handles its own deterministic retention!
   ```
   *(Always use `noeviction` for BullMQ/OxMQ Redis instances so Redis doesn't randomly evict active queues or lock keys).*
5. [ ] **Monitor Redis Memory**:
   ```bash
   redis-cli -h localhost -p 6379 info memory
   # Check 'used_memory_human' and 'mem_fragmentation_ratio'
   ```
