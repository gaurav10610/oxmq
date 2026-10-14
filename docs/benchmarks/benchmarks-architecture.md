# OxMQ & BullMQ Architectural Comparison & Benchmarks

This document provides a factual, technically grounded engineering comparison of **OxMQ** (Java 21+) and **BullMQ** (Node.js / TypeScript), examining how both engines leverage Redis, their concurrency models, and empirical scale benchmarks.

---

## 📊 Architectural Comparison: OxMQ vs. BullMQ

Because OxMQ directly executes BullMQ's 49 official Lua scripts and adheres to the identical Redis key schema, both engines provide 100% functional parity while leveraging the strengths of their respective host runtimes:

| Capability | 🐂 **OxMQ** (Java 21+) | 🐂 **BullMQ** (Node.js / TypeScript) |
| :--- | :--- | :--- |
| **Target Runtime** | **Java 21+** (OpenJDK, Temurin, GraalVM) | **Node.js 16+** / TypeScript |
| **Concurrency Model** | **Project Loom Virtual Threads** (`newVirtualThreadPerTaskExecutor`) | **Single-Threaded Event Loop** (with Worker Threads for sandboxing) |
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

2. **Java 21 / OxMQ Virtual Threads**:
   - OxMQ dispatches every claimed task to an unpinned Java 21 Virtual Thread.
   - Virtual threads unmount during network delays, database queries, and blocking socket I/O with negligible memory overhead (~1 KB per thread).
   - Compute-heavy and blocking I/O tasks run across all available CPU cores concurrently without child-process IPC overhead.

### Polyglot Coexistence on the Same Redis Cluster

Because OxMQ uses BullMQ's exact Lua scripts and Redis key schemas:
- A TypeScript/Node.js API can enqueue jobs with BullMQ that an OxMQ worker in Java processes.
- A Java microservice can enqueue jobs with OxMQ that a Python or Node.js BullMQ worker processes.
- Both can be monitored simultaneously on the same Bull-Board dashboard!

---

## ⚡ Empirical Scale & DAG Benchmarks

The following benchmarks were captured directly from live test executions on Java 21 LTS with Redis 7:

| Scenario | Total Nodes | Concurrency | Enqueue Time | Ingestion Rate | Resolution Time | Throughput | Redis RAM | Verified |
| :--- | :---: | :---: | :---: | :---: | :---: | :---: | :---: | :---: |
| **Wide DAG (1 Parent + 100 Children)** | 101 | 100 VTs | 111 ms | 909.91/s | 296 ms | 341.22/s | 220 KB | ✅ 100% |
| **Wide DAG (1 Parent + 500 Children)** | 501 | 100 VTs | 238 ms | 2,105.04/s | 706 ms | 709.63/s | 557 KB | ✅ 100% |
| **Wide DAG (1 Parent + 1,000 Children)** | 1,001 | 100 VTs | 447 ms | 2,239.37/s | 1,121 ms | 892.95/s | 1.05 MB | ✅ 100% |
| **Wide DAG (1 Parent + 2,500 Children)** | 2,501 | 100 VTs | 834 ms | 2,998.80/s | 2,540 ms | 984.65/s | 2.58 MB | ✅ 100% |
| **Wide DAG (1 Parent + 5,000 Children)** | 5,001 | 100 VTs | 1,699 ms | 2,943.50/s | 4,932 ms | 1,013.99/s | 5.15 MB | ✅ 100% |
| **3-Tier Tree (1 Root $\rightarrow$ 20 Branches $\rightarrow$ 2,000 Leaves)** | 2,021 | 100 VTs | 661 ms | 3,057.49/s | 1,997 ms | 1,012.02/s | 2.01 MB | ✅ 100% |
| **Flat Queue Ingestion (10,000 Jobs)** | 10,000 | 200 VTs | 3,173 ms | 3,151.59/s | 9,223 ms | 1,084.25/s | 6.57 MB | ✅ 100% |
| **Batch Dequeue (`batchSize=100`)** | 10,000 | 100 VTs | 3,188 ms | 3,136.76/s | 3,604 ms | 2,774.69/s | 6.77 MB | ✅ 100% |

### Key Takeaways for System Designers
1. **Redis Footprint**: Costs **~672 Bytes** per flat job and **~1,054 Bytes** per DAG tree node (including child result hash records). 1,000,000 in-flight jobs fit in under 1.1 GB of Redis RAM.
2. **DAG Integrity**: Zero dropped dependencies across 5,000 parallel children with automatic result passing to the parent job upon completion.
3. **Batch Dequeue Efficiency**: `OxmqBatchWorker` achieves **2,774+ jobs/second** by reducing Redis network round-trips by 100x.
4. **Loom Memory Footprint**: 200 concurrent worker threads execute within **89 MB peak JVM heap**.

For full capacity sizing formulas and cloud cost planning, see the [Operations Benchmarks & Sizing Guide](/operations/benchmarks).

---

## 🧪 Measuring Performance in Your Environment

You can execute the scale runner directly on your machines:

```bash
# Run full scale & DAG benchmark suite
./mvnw compile exec:java -pl oxmq-benchmarks \
  -Dexec.mainClass="io.oxmq.benchmarks.DagScaleBenchmarkRunner"

# Run JMH microbenchmarks
java -jar oxmq-benchmarks/target/oxmq-benchmarks-1.0.0.jar
```
