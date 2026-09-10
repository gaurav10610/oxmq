# OxMQ & BullMQ Architectural Comparison

This document provides a factual, technically grounded engineering comparison of **OxMQ** (Java 21+) and **BullMQ** (Node.js / TypeScript), examining how both engines leverage Redis, their concurrency models, and empirical benchmarking with JMH.

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

## 🧪 Measuring Performance in Your Environment

Because throughput and latency depend significantly on network round-trip time (RTT), payload size, Redis persistence configuration (AOF vs RDB), and worker business logic, **we encourage running empirical benchmarks on your target hardware**:

OxMQ includes a dedicated **JMH (Java Microbenchmark Harness)** module in `oxmq-benchmarks/`:

```bash
# Compile benchmark suite
./mvnw clean test-compile -pl oxmq-benchmarks

# Run benchmarks against your local or remote Redis instance
java -jar oxmq-benchmarks/target/oxmq-benchmarks-1.0.0.jar
```
