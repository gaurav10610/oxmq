# OxMQ & BullMQ Architectural Comparison & Benchmarks

This document provides a factual, technically grounded engineering comparison of **OxMQ** (Java 17+ / Java 21+) and **BullMQ** (Node.js / TypeScript), examining how both engines leverage Redis, their concurrency models, and empirical scale benchmarks.

---

## 📊 Architectural Comparison: OxMQ vs. BullMQ

Because OxMQ directly executes BullMQ's 49 official Lua scripts and adheres to the identical Redis key schema, both engines provide 100% functional parity while leveraging the strengths of their respective host runtimes:

| Capability | 🐂 **OxMQ** (Java 17+ / Java 21+) | 🐂 **BullMQ** (Node.js / TypeScript) |
| :--- | :--- | :--- |
| **Target Runtime** | **Java 17+ LTS** (with native **Java 21+ Project Loom Virtual Threads**) | **Node.js 16+** / TypeScript |
| **Concurrency Model** | **Project Loom Virtual Threads** (Java 21+) / Platform Thread Pool (Java 17) | **Single-Threaded Event Loop** (with Worker Threads for sandboxing) |
| **I/O Blocking Behavior** | Virtual threads unmount from carrier threads during blocking I/O | Non-blocking async/await promises |
| **CPU-Intensive Tasks** | Multi-core JVM execution across all CPU cores natively | Requires spawning separate OS sandboxed worker processes |
| **Redis Lua Scripts** | **Direct execution of 49 official BullMQ v5 Lua scripts** | **Official BullMQ v5 Lua scripts** |
| **Redis Key Topology** | Standard `bull:<queue>:*` naming convention | Standard `bull:<queue>:*` naming convention |
| **Wire Protocol Interop** | 100% compatible (can produce or consume across languages) | 100% compatible (can produce or consume across languages) |
| **Job Lifecycle States** | WAITING, ACTIVE, DELAYED, WAITING_CHILDREN, COMPLETED, FAILED, PAUSED, STALLED | WAITING, ACTIVE, DELAYED, WAITING_CHILDREN, COMPLETED, FAILED, PAUSED, STALLED |
| **DAG Workflows** | Native `FlowProducer` with parent-child trees | Native `FlowProducer` with parent-child trees |
| **Rate Limiting** | Sliding window token bucket with `groupKey` | Sliding window token bucket with `groupKey` |
| **Web Dashboard** | Native compatibility with **Bull-Board UI** | Native compatibility with **Bull-Board UI** |
| **Framework Integration** | **Spring Boot 3+ Starter** (`@OxmqListener`, Actuator) | Express, Fastify, NestJS BullMQ module |
| **License** | **Apache 2.0** | **MIT** |

---

## 🔬 Deep Dive: Concurrency Architecture

### Single-Threaded Event Loop vs. Multi-Core Loom Virtual Threads

1. **Node.js / BullMQ Event Loop**:
   - BullMQ runs on Node.js's single-threaded event loop.
   - Ideal for lightweight async I/O.
   - For CPU-heavy work (e.g. video processing, cryptography, PDF generation, large JSON parsing), Node.js workers must fork separate child processes ("sandboxed workers") to avoid blocking the main event loop.

2. **Java 17+ / Java 21 Loom Virtual Threads**:
   - When running on Java 21+, OxMQ dispatches every claimed task to an unpinned Virtual Thread (`Executors.newVirtualThreadPerTaskExecutor()`).
   - When running on Java 17 LTS, OxMQ uses dynamic platform thread pools with graceful fallback via `DispatcherExecutorFactory`.
   - Virtual threads unmount during network delays, database queries, and blocking socket I/O with negligible memory overhead (~1 KB per thread).
   - Compute-heavy and blocking I/O tasks run across all available CPU cores concurrently without child-process IPC overhead.

### Polyglot Coexistence on the Same Redis Cluster

Because OxMQ uses BullMQ's exact Lua scripts and Redis key schemas:
- A TypeScript/Node.js API can enqueue jobs with BullMQ that an OxMQ worker in Java processes.
- A Java microservice can enqueue jobs with OxMQ that a Python or Node.js BullMQ worker processes.
- Both can be monitored simultaneously on the same Bull-Board dashboard!

---

## ⚡ Empirical Scale, Java 17 vs 21 & Inflection Points

The following benchmarks were captured directly from live test executions on both **Java 17 LTS (Platform Threads)** and **Java 21 LTS (Project Loom Virtual Threads)** with Redis 7:

| Scenario / Topology | Java 17 Throughput | Java 21 Throughput | Peak Ingestion Rate | Redis RAM Delta | Cost / Node | Verification |
| :--- | :---: | :---: | :---: | :---: | :---: | :---: |
| **Wide DAG (1 Parent + 100 Children)** | 573.9 jobs/s | 567.4 jobs/s | 1,836.4 nodes/s | 134 KB | 1,364 B | ✅ 100% |
| **Wide DAG (1 Parent + 500 Children)** | 926.1 jobs/s | 931.2 jobs/s | 2,846.6 nodes/s | 502 KB | 1,026 B | ✅ 100% |
| **Wide DAG (1 Parent + 1,000 Children)** | 1,053.7 jobs/s | 1,041.6 jobs/s | 3,128.1 nodes/s | 940 KB | 961 B | ✅ 100% |
| **Wide DAG (1 Parent + 2,500 Children)** | 1,068.8 jobs/s | 1,055.7 jobs/s | 3,169.8 nodes/s | 2.16 MB | 886 B | ✅ 100% |
| **Wide DAG (1 Parent + 5,000 Children)** | 1,088.1 jobs/s | 1,078.7 jobs/s | 3,193.5 nodes/s | 4.52 MB | 926 B | ✅ 100% |
| **Wide DAG (1 Parent + 10,000 Children)**| **1,092.6 jobs/s** | **1,084.1 jobs/s** | **3,230.4 nodes/s** | **8.87 MB** | **908 B** | ✅ 100% |
| **Batch Dequeue (`batchSize=100`)** | 2,896.0 jobs/s | 2,853.1 jobs/s | 3,150.0 jobs/s | 6.77 MB | 693 B | ✅ 100% |
| **Batch Dequeue (`batchSize=1000`)** | **2,918.9 jobs/s [PEAK]** | **2,886.8 jobs/s [PEAK]** | 3,180.0 jobs/s | 6.77 MB | 693 B | ✅ 100% |

### 🔍 Key Inflection Points & Takeaways
1. **Concurrency Saturation (Inflection Point: $C = 50 - 100$)**:
   - Single-worker queue polling hits a Redis network round-trip ceiling at **~1,110 – 1,118 jobs/s**. Beyond 100 workers, single queue polling plateaus. Use `OxmqBatchWorker` to achieve **2,918+ jobs/s**.
2. **DAG Fan-out Capacity (Scale Limit: 10,000+ Children)**:
   - Zero dropped tasks or race conditions across 10,000 parallel children with automatic result passing to the parent job upon completion. Ingestion plateaus at **~3,230 nodes/s** (Redis single TCP buffer limit).
3. **Memory Asymptotic Stabilization**:
   - At scale ($\ge 2,500$ nodes), Redis memory consumption asymptotically stabilizes at **~904 – 908 Bytes/node**.
4. **Loom Virtual Thread Isolation**:
   - Under realistic 10ms I/O delays, Java 21 Virtual Threads maintain a **strictly flat 31 OS threads** up to 800 workers, while Java 17 spawns OS threads in its platform thread pool.

For full capacity sizing formulas, cloud cost budgeting, and detailed charts, see the [Operations Benchmarks & Sizing Guide](/operations/benchmarks).

---

## 🧪 Measuring Performance in Your Environment

You can execute the scale runner directly on your machines:

```bash
# Run full scale & DAG benchmark suite
./mvnw compile exec:java -pl oxmq-benchmarks \
  -Dexec.mainClass="io.oxmq.benchmarks.DagScaleBenchmarkRunner"

# Run inflection point & degradation benchmark suite
./mvnw compile exec:java -pl oxmq-benchmarks \
  -Dexec.mainClass="io.oxmq.benchmarks.InflectionPointBenchmarkRunner"

# Run JMH microbenchmarks
java -jar oxmq-benchmarks/target/oxmq-benchmarks-1.2.0.jar
```
