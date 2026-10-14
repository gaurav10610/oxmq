# OxMQ Peak Scale, DAG Tree & Inflection Point Benchmarks (Java 17 & Java 21)

This document delivers comprehensive, real-world empirical benchmarks comparing **OxMQ on Java 17 LTS (Platform Threads)** and **Java 21 LTS (Project Loom Virtual Threads)** against **Redis 7**.

It highlights **absolute peak throughput numbers**, **exact Redis memory footprints**, and the **critical inflection points** after which performance plateaus or degrades.

---

## 🔬 Test Methodology & Environment

All metrics were captured using [`io.oxmq.benchmarks.InflectionPointBenchmarkRunner`](https://github.com/gaurav10610/oxmq/blob/main/oxmq-benchmarks/src/main/java/io/oxmq/benchmarks/InflectionPointBenchmarkRunner.java) executing against a dedicated Redis 7 container on macOS Apple Silicon.

- **Isolation**: Each test scenario executes on a pristine Redis state (`FLUSHDB`) followed by a full JVM garbage collection pass (`System.gc()`) to eliminate cache pollution.
- **Byte Accuracy**: Memory is measured using byte-level deltas parsed directly from Redis `INFO memory` (`used_memory:<bytes>`).
- **OS Thread Telemetry**: Thread counts are tracked using Java's `ManagementFactory.getThreadMXBean()`.
- **100% Verification**: Every DAG run verifies zero dropped child tasks, complete parent promotion, and byte-accurate return value population in `parentJob.getChildrenValues()`.

---

## 📊 Summary: Java 17 vs. Java 21 Empirical Comparison

| Metric / Scenario | ☕ **Java 17 LTS (Platform Threads)** | 🐂 **Java 21 LTS (Loom Virtual Threads)** | Strategic Analysis |
| :--- | :---: | :---: | :--- |
| **Max Verified DAG Tree** | **10,001 Nodes** (1 Parent + 10,000 Children) | **10,001 Nodes** (1 Parent + 10,000 Children) | Both runtimes scale to 10,000+ nodes with 100% verification |
| **Peak DAG Ingestion Rate** | **3,194.63 nodes/sec** (at 7,500 children) | **3,230.40 nodes/sec** (at 7,500 children) | Single pipeline buffer saturation point |
| **Peak DAG Resolution Rate** | **1,092.65 jobs/sec** (at 10,000 children) | **1,084.12 jobs/sec** (at 10,000 children) | Flat throughput across both runtimes |
| **Redis RAM Delta (10k DAG)**| **8,868.5 KB** (~8.66 MB) | **8,873.0 KB** (~8.66 MB) | Ultra-consistent: **~908 Bytes/node** at scale |
| **Peak Batch Dequeue Rate** | **2,918.86 jobs/sec** (`batchSize=1000`) | **2,886.84 jobs/sec** (`batchSize=1000`) | **~2.6x faster** than 1-by-1 polling |
| **Batch Sweet Spot (`batchSize=100`)**| **2,896.00 jobs/sec** (3,453 ms) | **2,853.10 jobs/sec** (3,505 ms) | Ideal balance of throughput vs batch latency |
| **OS Thread Count (at 800 Workers)** | **Climbs to 42+ OS Threads** | **Strictly Fixed at 31 OS Threads** | Virtual Threads insulate OS from thread explosion |
| **Peak JVM Heap under Load** | **68 – 118 MB** | **33 – 79 MB** | Loom achieves lower baseline heap under high task count |

---

## 📈 Deep-Dive: Absolute Peaks & Inflection Points

### 1. Engine Core Throughput & Worker Concurrency (0ms Simulated I/O)

This suite pushes a flat queue of 5,000 jobs through a single worker at concurrency levels from 10 to 800 with zero artificial delay:

| Concurrency | Java 17 Throughput | Java 17 Exec Time | Java 21 Throughput | Java 21 Exec Time | Java 17 Peak Heap | Java 21 Peak Heap |
| :---: | :---: | :---: | :---: | :---: | :---: | :---: |
| **10** | 998.8 jobs/s | 5,006 ms | 983.5 jobs/s | 5,084 ms | 32 MB | 32 MB |
| **25** | 1,041.0 jobs/s | 4,803 ms | 1,051.3 jobs/s | 4,756 ms | 69 MB | 44 MB |
| **50** | 1,110.1 jobs/s | 4,504 ms | 1,103.5 jobs/s | 4,531 ms | 69 MB | 32 MB |
| **100** | 1,110.6 jobs/s | 4,502 ms | **1,112.1 jobs/s [PEAK]** | **4,496 ms** | 118 MB | 33 MB |
| **200** | 1,097.7 jobs/s | 4,555 ms | 1,102.0 jobs/s | 4,537 ms | 47 MB | 33 MB |
| **400** | **1,118.1 jobs/s [PEAK]** | **4,472 ms** | 1,103.0 jobs/s | 4,533 ms | 63 MB | 33 MB |
| **800** | 1,115.6 jobs/s | 4,482 ms | 1,106.4 jobs/s | 4,519 ms | 71 MB | 54 MB |

```
Throughput (jobs/sec)
1,200 ┼                                        ╭─────────────────── Java 17 Peak: 1,118.1/s (at 400)
      │                   ╭────────────────────┴─────────────────── Java 21 Peak: 1,112.1/s (at 100)
1,100 ┼             ╭─────╯  (Saturation Plateau: ~1,110 jobs/s)
      │       ╭─────╯
1,000 ┼───────╯
      │
  900 ┼─────┬─────────┬─────────┬─────────┬─────────┬─────────┬─────────
      0    10        25        50        100       200       400       800  Concurrency
```

#### 🔍 Inflection Point Analysis:
* **Concurrency Inflection Point ($C = 50 – 100$)**:
  - Below 50 concurrency, throughput is bottlenecked by concurrency starvation ($< 1,050$ jobs/s).
  - At **50–100 concurrency**, throughput hits the **Redis single-connection network round-trip ceiling** (~1,110 jobs/s on loopback).
* **Diminishing Returns Plateau ($C > 100$)**:
  - Increasing concurrency beyond 100 on a single queue consumer with 0ms tasks yields zero additional throughput because the single connection Redis poller cannot deliver jobs faster than 1.1 kHz.
  - To break past 1,110 jobs/s, applications must use **Batch Dequeue** (`OxmqBatchWorker`) or multi-partition consumers.

---

### 2. Massive DAG Fan-Out Scaling (1 Parent + N Children up to 10,000 Nodes)

Tests parent-child dependency trees with fan-outs from 100 to 10,000 parallel children at concurrency 100:

| Children | Total Nodes | Java 17 Ingestion | Java 21 Ingestion | Java 17 Resolution | Java 21 Resolution | Redis RAM Delta | Cost / Node | Verified |
| :---: | :---: | :---: | :---: | :---: | :---: | :---: | :---: | :---: |
| **100** | 101 | 1,836.4/s (55 ms) | 1,603.2/s (63 ms) | 573.9/s (176 ms) | 567.4/s (178 ms) | 134.6 KB | 1,364 B | ✅ 100% |
| **500** | 501 | 2,609.4/s (192 ms) | 2,846.6/s (176 ms) | 926.1/s (541 ms) | 931.2/s (538 ms) | 502.4 KB | 1,026 B | ✅ 100% |
| **1,000** | 1,001 | 3,128.1/s (320 ms) | 2,750.0/s (364 ms) | 1,053.7/s (950 ms) | 1,041.6/s (961 ms) | 940.1 KB | 961 B | ✅ 100% |
| **2,500** | 2,501 | 3,169.8/s (789 ms) | 3,118.5/s (802 ms) | 1,068.8/s (2,340 ms) | 1,055.7/s (2,369 ms) | 2,164.5 KB | 886 B | ✅ 100% |
| **5,000** | 5,001 | 3,177.3/s (1,574 ms) | 3,193.5/s (1,566 ms) | 1,088.1/s (4,596 ms) | 1,078.7/s (4,636 ms) | 4,525.1 KB | 926 B | ✅ 100% |
| **7,500** | 7,501 | **3,194.6/s (2,348 ms)** | **3,230.4/s (2,322 ms)** | 1,090.7/s (6,877 ms) | 1,078.3/s (6,956 ms) | 6,624.6 KB | 904 B | ✅ 100% |
| **10,000** | 10,001 | 3,165.9/s (3,159 ms) | 3,219.9/s (3,106 ms) | **1,092.6/s (9,153 ms)** | **1,084.1/s (9,225 ms)** | 8,868.5 KB | 908 B | ✅ 100% |

#### 🔍 Inflection Point Analysis:
* **Ingestion Saturation Inflection Point ($N = 7,500$)**:
  - Ingestion rate scales steeply from 1,600 nodes/s at 100 nodes up to **~3,230 nodes/s at 7,500 nodes**.
  - Above 7,500 nodes, ingestion throughput reaches the Redis single-command TCP packet buffer ceiling (~3,200 nodes/s).
* **Resolution Throughput Saturation Point ($N \ge 1,000$)**:
  - Worker resolution throughput reaches full pipe capacity at **1,000 children (~1,050 jobs/s)** and stays rock-solid all the way to **10,000 children (~1,092 jobs/s)**.
  - Zero dropped tasks, zero race conditions, and complete collection of all 10,000 return values in the parent's `getChildrenValues()`.
* **Redis Memory Asymptotic Stabilization**:
  - At small tree sizes ($N \le 100$), Redis overhead is ~1,364 Bytes/node due to hash metadata headers and sorted set initializations.
  - At scale ($N \ge 2,500$), per-node cost asymptotically stabilizes at **~904 – 908 Bytes/node**.
  - A massive 10,001-node DAG tree consumes only **8.66 MB** of Redis RAM!

---

### 3. Batch Dequeue Size Optimization (`OxmqBatchWorker`)

Measures 10,000 jobs ingested and consumed via `OxmqBatchWorker` across batch sizes from 10 to 1,000:

| Batch Size | Java 17 Throughput | Java 21 Throughput | Batches Processed | Dequeue Time (Java 17) | Dequeue Time (Java 21) | Latency / Batch |
| :---: | :---: | :---: | :---: | :---: | :---: | :---: |
| **10** | 2,837.7 jobs/s | 2,801.9 jobs/s | 1,000 batches | 3,524 ms | 3,569 ms | ~3.5 ms |
| **25** | 2,766.3 jobs/s | 2,818.5 jobs/s | 400 batches | 3,615 ms | 3,548 ms | ~8.9 ms |
| **50** | 2,844.1 jobs/s | 2,840.9 jobs/s | 200 batches | 3,516 ms | 3,520 ms | ~17.6 ms |
| **100** | **2,896.0 jobs/s** | **2,853.1 jobs/s** | 100 batches | **3,453 ms** | **3,505 ms** | ~34.8 ms |
| **200** | 2,847.4 jobs/s | 2,849.0 jobs/s | 50 batches | 3,512 ms | 3,510 ms | ~70.2 ms |
| **500** | **2,896.0 jobs/s** | 2,867.8 jobs/s | 20 batches | 3,453 ms | 3,487 ms | ~174.3 ms |
| **1,000** | **2,918.9 jobs/s [PEAK]** | **2,886.8 jobs/s [PEAK]** | 10 batches | 3,426 ms | 3,464 ms | ~344.5 ms |

#### 🔍 Inflection Point Analysis:
* **The Bulk Advantage**: Batch workers jump from ~1,100 jobs/s (1-by-1 worker) to **~2,850 – 2,918 jobs/s** (**2.6x throughput gain**) by cutting Redis network round-trips by 90–99%.
* **The Production Sweet Spot (`batchSize = 100 – 200`)**:
  - `batchSize = 100` achieves **99.2% of peak throughput** while keeping batch dispatch latency under **35 ms**.
* **Latency Inflection Point (`batchSize > 500`)**:
  - While `batchSize = 1,000` achieves peak throughput (~2,919 jobs/s), each batch takes ~345 ms to process. In latency-sensitive pipelines, setting batchSize > 200 introduces head-of-line delays for early jobs in the batch.

---

### 4. Realistic Microservice I/O Concurrency (10ms Simulated I/O)

In real microservices, worker tasks perform network requests, database transactions, or external API calls. This suite simulates a **10ms blocking I/O call per task** across 1,000 jobs:

| Concurrency | Java 17 Throughput | Java 17 Peak OS Threads | Java 21 Throughput | Java 21 Peak OS Threads | Java 17 Peak Heap | Java 21 Peak Heap |
| :---: | :---: | :---: | :---: | :---: | :---: | :---: |
| **10** | 734.2 jobs/s | 35 threads | 792.4 jobs/s | **31 threads** | 67 MB | **35 MB** |
| **25** | 1,127.4 jobs/s | 41 threads | 1,095.3 jobs/s | **31 threads** | 66 MB | **35 MB** |
| **50** | 1,136.4 jobs/s | 41 threads | **1,128.7 jobs/s [PEAK]** | **31 threads** | 66 MB | **35 MB** |
| **100** | 1,112.3 jobs/s | 41 threads | 1,106.2 jobs/s | **31 threads** | 67 MB | 79 MB |
| **200** | 1,117.3 jobs/s | 41 threads | 1,114.8 jobs/s | **31 threads** | 67 MB | 78 MB |
| **400** | 1,157.4 jobs/s | 41 threads | 1,112.3 jobs/s | **31 threads** | 68 MB | 78 MB |
| **800** | **1,173.7 jobs/s [PEAK]** | **42 threads** | 1,067.2 jobs/s | **31 threads** | 68 MB | 78 MB |

#### 🔍 Inflection Point Analysis:
* **The OS Thread Advantage of Java 21 Loom**:
  - In Java 21, the OS thread count is **strictly fixed at 31 OS threads** regardless of whether worker concurrency is 10, 100, 400, or 800. Virtual Threads unmount from carrier threads during the 10ms I/O delay in user space.
  - In Java 17, platform threads must be spawned and maintained in the OS thread pool, consuming ~1 MB native stack per active thread.
* **Degradation Point in Java 21 ($C = 800$)**:
  - Java 21 throughput peaks at 50 workers (~1,128.7 jobs/s) and shows a slight degradation at 800 workers (1,067.2 jobs/s, ~5.4% drop) due to Virtual Thread scheduler coordination on a single queue consumer.

---

## 💾 Capacity Planning & Redis Sizing Matrix

Based on our empirical measurements, use this guide to accurately size your production Redis cluster:

| In-Flight Job Scale | Workload Profile | Measured Cost / Node | Redis RAM Needed | Recommended Cloud Tier | Estimated Cost |
| :--- | :--- | :---: | :---: | :--- | :---: |
| **10,000 Jobs** | Standard Queue | ~672 Bytes | **~6.6 MB** | Redis Cloud Free (30 MB) / `cache.t4g.micro` | **$0 – $12/mo** |
| **10,000 Jobs** | Complex DAG Tree | ~908 Bytes | **~9.1 MB** | Redis Cloud Free (30 MB) / `cache.t4g.micro` | **$0 – $12/mo** |
| **100,000 Jobs** | Standard Queue | ~672 Bytes | **~67.2 MB** | AWS `cache.t4g.micro` (0.5 GB) | **~$12/mo** |
| **100,000 Jobs** | Complex DAG Tree | ~908 Bytes | **~90.8 MB** | AWS `cache.t4g.micro` (0.5 GB) | **~$12/mo** |
| **1,000,000 Jobs** | Standard Queue | ~672 Bytes | **~672 MB** | AWS `cache.t4g.small` (1.37 GB) | **~$25/mo** |
| **1,000,000 Jobs** | Complex DAG Tree | ~908 Bytes | **~908 MB** | AWS `cache.t4g.small` (1.37 GB) | **~$25/mo** |
| **10,000,000 Jobs** | Standard Queue | ~672 Bytes | **~6.72 GB** | AWS `cache.m7g.large` (13.07 GB) | **~$170/mo** |
| **10,000,000 Jobs** | Complex DAG Tree | ~908 Bytes | **~9.08 GB** | AWS `cache.m7g.large` (13.07 GB) | **~$170/mo** |

---

## 🎯 Architecture Decision Guide: Java 17 vs. Java 21

### When to deploy on Java 17 LTS:
1. **Existing Enterprise Base**: If your organization has standardized on Java 17 LTS containers, OxMQ runs with **100% functional parity and identical Redis wire compatibility**.
2. **Batch Ingestion Workloads**: `OxmqBatchWorker` achieves **2,918+ jobs/sec** on Java 17, matching Java 21 throughput.
3. **DAG Workflows up to 10,000 nodes**: Java 17 enqueues and resolves 10,000 children with zero dropped dependencies.

### When to upgrade to Java 21 LTS:
1. **High Concurrency Microservices**: If individual workers perform blocking I/O (REST calls, database queries), Java 21 Loom Virtual Threads isolate the host OS from thread starvation and context-switch penalties.
2. **Dense Multi-Queue Containers**: Spawning 100+ separate queue workers inside a single JVM container consumes < 80 MB heap on Java 21 without exhausting native OS thread limits.

---

## 🏃 Reproducing These Benchmarks

Run the test suite on your own infrastructure:

```bash
# 1. Start Redis
docker run -d --name oxmq-redis -p 6379:6379 redis:7-alpine

# 2. Run on Java 17 LTS
sdk use java 17.0.20-tem
./mvnw compile exec:java -pl oxmq-benchmarks \
  -Dexec.mainClass="io.oxmq.benchmarks.InflectionPointBenchmarkRunner"

# 3. Run on Java 21 LTS
sdk use java 21.0.0.0+35-tem
./mvnw compile exec:java -pl oxmq-benchmarks \
  -Dexec.mainClass="io.oxmq.benchmarks.InflectionPointBenchmarkRunner"
```
