# OxMQ Scale, DAG Tree & Performance Benchmarks

This document provides real-world, empirical scale and performance benchmarks for **OxMQ** running on **Java 21 LTS** with **Project Loom Virtual Threads** against **Redis 7**.

Whether you are evaluating OxMQ as your primary distributed message queue or sizing Redis clusters for high-volume production workloads, this guide delivers byte-accurate memory measurements, DAG parent-child scale limits, throughput numbers, and capacity planning formulas.

---

## 📊 Official Scale & DAG Performance Report

All metrics below represent **real, live execution benchmarks** captured using `io.oxmq.benchmarks.DagScaleBenchmarkRunner` against a standard Redis 7 instance. Each run was executed on a clean Redis state (`FLUSHDB`), measuring byte-level Redis delta via `INFO memory`, JVM heap consumption, and end-to-end task completion verification:

| Benchmark Scenario | Total Nodes | Concurrency | Enqueue Time | Ingestion Rate | Resolution Time | Throughput | Redis RAM (Delta) | Cost / Node | Peak Heap | Verified |
| :--- | :---: | :---: | :---: | :---: | :---: | :---: | :---: | :---: | :---: | :---: |
| **Wide DAG (1 Parent + 100 Children)** | 101 | 100 VTs | 111 ms | 909.91/s | 296 ms | 341.22/s | 220.42 KB | 2,234 B | 51 MB | ✅ 100% |
| **Wide DAG (1 Parent + 500 Children)** | 501 | 100 VTs | 238 ms | 2,105.04/s | 706 ms | 709.63/s | 557.62 KB | 1,139 B | 67 MB | ✅ 100% |
| **Wide DAG (1 Parent + 1,000 Children)** | 1,001 | 100 VTs | 447 ms | 2,239.37/s | 1,121 ms | 892.95/s | 1,048.45 KB | 1,072 B | 50 MB | ✅ 100% |
| **Wide DAG (1 Parent + 2,500 Children)** | 2,501 | 100 VTs | 834 ms | 2,998.80/s | 2,540 ms | 984.65/s | 2,583.98 KB | 1,057 B | 70 MB | ✅ 100% |
| **Wide DAG (1 Parent + 5,000 Children)** | 5,001 | 100 VTs | 1,699 ms | 2,943.50/s | 4,932 ms | 1,013.99/s | 5,150.51 KB | 1,054 B | 80 MB | ✅ 100% |
| **3-Tier Tree (1 Root $\rightarrow$ 10 Branches $\rightarrow$ 500 Leaves)** | 511 | 100 VTs | 181 ms | 2,823.20/s | 541 ms | 944.55/s | 520.00 KB | 1,042 B | 70 MB | ✅ 100% |
| **3-Tier Tree (1 Root $\rightarrow$ 20 Branches $\rightarrow$ 2,000 Leaves)** | 2,021 | 100 VTs | 661 ms | 3,057.49/s | 1,997 ms | 1,012.02/s | 2,005.38 KB | 1,016 B | 89 MB | ✅ 100% |
| **Flat Queue Ingestion & Worker Drain** | 10,000 | 200 VTs | 3,173 ms | 3,151.59/s | 9,223 ms | 1,084.25/s | 6,567.13 KB | 672 B | 89 MB | ✅ 100% |
| **High-Throughput Batch Dequeue (`batchSize=100`)** | 10,000 | 100 VTs | 3,188 ms | 3,136.76/s | 3,604 ms | 2,774.69/s | 6,773.89 KB | 693 B | 89 MB | ✅ 100% |

> **Verification Criteria**: Every DAG execution verifies that:
> 1. Exactly 100% of children completed execution without any dropped or lost tasks.
> 2. The parent job's `job.getChildrenValues()` contains all child return values mapped to their job keys.
> 3. Multi-tier intermediate branch coordinators accurately aggregated leaf results before unblocking the master root coordinator.

---

## 🌲 Deep-Dive: Parent-Child DAG Tree Scale

### Massive Fan-Out Support (5,000+ Parallel Children)
In enterprise workflows (e.g. video transcode chunking, e-commerce catalog sync, distributed map-reduce jobs), a single orchestrator job often fans out thousands of child tasks.

* **Linear Ingestion Scaling**: Enqueuing a massive 5,001-node DAG tree requires only **1.69 seconds** (~2,944 nodes/sec) using pipelined Redis transactions and BullMQ's native `addChildren` Lua scripts.
* **Rapid Concurrent Resolution**: A worker pool with 100 Virtual Threads consumed and resolved 5,000 parallel children in **4.93 seconds** (~1,014 jobs/sec).
* **Zero Dependency Race Conditions**: When the 5,000th child completes, Redis atomically transitions the parent from `WAITING_CHILDREN` to `WAITING`, with zero polling loops or external locking.

### Multi-Tier Hierarchical Trees (2,000+ Nodes)
OxMQ supports arbitrary levels of nesting ($Root \rightarrow Intermediate\ Parents \rightarrow Leaf\ Tasks$):
* A 3-tier hierarchy with **1 Master Root $\rightarrow$ 20 Branch Coordinators $\rightarrow$ 2,000 Leaf Tasks (2,021 total nodes)** was enqueued in **661 ms** (~3,058 nodes/sec) and resolved in **1.99 seconds**.
* Each tier automatically unblocks the next level upon completion, propagating intermediate aggregated results up the tree.

---

## 💾 Byte-Accurate Redis Memory Overhead

Memory consumption in Redis is a critical factor for infrastructure cost. OxMQ queries Redis `INFO memory` before and after enqueuing to calculate the exact byte footprint per task:

```
+-------------------------------------------------------------------------------+
| Job Type         | Redis Keys Created Per Job               | Bytes / Node    |
+-------------------------------------------------------------------------------+
| Standard Flat    | Job Hash, ID Counter, Wait ZSet, Logs     | ~672 Bytes      |
| DAG Tree Node    | Above + ParentKey, Children Hash, Results | ~1,016 - 1,054 B|
+-------------------------------------------------------------------------------+
```

### What is Stored per Job Node in Redis?
1. **Job Hash** (`bull:<queue>:<jobId>`): Stores name, JSON data payload, options, timestamp, attempts, and status.
2. **State Indexes**:
   - `bull:<queue>:wait` / `bull:<queue>:active`: List/ZSet maintaining FIFO ordering.
   - `bull:<queue>:waiting-children`: Sorted set holding parents awaiting child completion.
   - `bull:<queue>:dependencies`: Hash maintaining parent dependency counts.
   - `bull:<queue>:<jobId>:processed`: Hash storing serialized return values of children.

---

## 📐 Capacity Sizing & Cloud Cost Matrix

Use this matrix to size your Redis instances and budget infrastructure costs for production workloads:

| Active In-Flight Jobs | Workload Type | Redis RAM Required | Recommended AWS ElastiCache | Recommended Redis Cloud | Estimated Monthly Cost |
| :--- | :--- | :---: | :--- | :--- | :---: |
| **10,000 Jobs** | Standard Queue | **~6.6 MB** | `cache.t4g.micro` (0.5 GB) | Free Tier (30 MB) | **$0 - $12 / mo** |
| **100,000 Jobs** | Standard Queue | **~67.2 MB** | `cache.t4g.micro` (0.5 GB) | 100 MB Plan | **~$12 / mo** |
| **100,000 Jobs** | Complex DAG Tree | **~105.4 MB** | `cache.t4g.small` (1.37 GB) | 250 MB Plan | **~$25 / mo** |
| **1,000,000 Jobs** | Standard Queue | **~672 MB** | `cache.t4g.medium` (3.09 GB) | 1 GB Plan | **~$50 / mo** |
| **1,000,000 Jobs** | Complex DAG Tree | **~1.05 GB** | `cache.t4g.medium` (3.09 GB) | 1.5 GB Plan | **~$65 / mo** |
| **10,000,000 Jobs** | Standard Queue | **~6.72 GB** | `cache.m7g.large` (13.07 GB) | 10 GB Plan | **~$170 / mo** |

> [!TIP] Optimizing Redis RAM with Auto-Pruning
> In production, configure job options to automatically trim completed and failed jobs:
> ```java
> JobOptions opts = JobOptions.builder()
>     .removeOnComplete(100) // Keep only last 100 finished jobs for inspection
>     .removeOnFail(1000)    // Keep only last 1000 failed jobs for debugging
>     .build();
> ```
> With auto-pruning enabled, your Redis footprint reflects only **currently active and waiting jobs**, dramatically lowering persistent memory requirements.

---

## 🧵 Java 21 Loom: Virtual Threads vs. Platform Threads

OxMQ is engineered from the ground up to leverage Java 21's Project Loom (`Executors.newVirtualThreadPerTaskExecutor()`).

### The Thread Memory Problem in Legacy Queues
In traditional Java message queues (RabbitMQ consumers, JMS listeners, Spring AMQP), each worker thread is a 1:1 mapping to an OS Platform Thread. An OS thread reserves a **1 MB native stack**:
* 200 Platform Threads = **200 MB of native RAM** just for thread stacks.
* 2,000 Platform Threads = **2 GB of native RAM**, causing thread exhaustion or `OutOfMemoryError: unable to create native thread`.

### The OxMQ Advantage: Virtual Threads
* **Stack Size**: Virtual Threads start with a dynamic heap chunk of only **~1 - 2 KB**.
* **Carrier Thread Unmounting**: When a worker blocks on network I/O, database queries, or external REST APIs, the virtual thread automatically unmounts from its carrier thread, freeing the CPU core for other tasks.
* **Empirical Heap Footprint**: During our 10,000-job concurrency tests with **200 Virtual Threads**, the peak JVM heap was only **89 MB**!

```
Platform Threads (Legacy):
[ 1 MB Stack ] x 1,000 Workers = ~1.0 GB RAM overhead

OxMQ Virtual Threads (Java 21):
[ 2 KB Stack ] x 1,000 Workers = ~2.0 MB RAM overhead  (500x Reduction!)
```

---

## ⚡ High-Throughput Batch Dequeue

For ultra-high ingestion scenarios (such as telemetry ingestion, batch database writes, or bulk event streaming), standard 1-by-1 job popping can saturate network latency.

OxMQ provides **`OxmqBatchWorker`** which dequeues batches of jobs in a single atomic Lua call:

* **1-by-1 Processing (200 Virtual Threads)**: ~1,084.25 jobs/second.
* **Batch Dequeue (`batchSize=100`)**: **2,774.69 jobs/second** (~2.5x higher throughput).
* **Network Efficiency**: Reduces Redis round-trips by 100x, allowing a single worker instance to ingest and process thousands of records per second.

---

## 🔬 Reproducing the Benchmarks

You can run this exact benchmark suite in your own infrastructure to verify performance against your Redis instance:

```bash
# 1. Ensure Redis is accessible (default: redis://localhost:6379)
docker run -d --name oxmq-redis -p 6379:6379 redis:7-alpine

# 2. Compile and run the suite with Java 21 LTS
./mvnw compile exec:java -pl oxmq-benchmarks \
  -Dexec.mainClass="io.oxmq.benchmarks.DagScaleBenchmarkRunner" \
  -Dredis.uri="redis://localhost:6379"
```

To run the JMH microbenchmark harness for nanosecond-level profiling:

```bash
./mvnw clean package -pl oxmq-benchmarks -DskipTests
java -jar oxmq-benchmarks/target/oxmq-benchmarks-1.0.0.jar -t 4 -wi 3 -i 5
```
