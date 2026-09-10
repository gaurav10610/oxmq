# ⚖️ Architectural Comparison: OxMQ & BullMQ
### *An Objective Engineering Evaluation of OxMQ (Java 21 Loom) and BullMQ (Node.js)*

**OxMQ** was created to bring the battle-tested, world-class queue architecture of **BullMQ** into the modern **Java 21+ enterprise ecosystem**.

Because OxMQ directly executes BullMQ's official 49 Lua scripts and uses the exact same Redis key hierarchy, it achieves 100% functional and wire compatibility with BullMQ while unlocking the advantages of Java 21's Project Loom Virtual Threads.

---

## 📊 Comprehensive Comparison Matrix

| Feature / Capability | 🐂 **OxMQ** (Java 21+) | 🐂 **BullMQ** (Node.js / TypeScript) |
| :--- | :---: | :---: |
| **Target Runtime** | **Java 21+** (JVM, OpenJDK, Temurin) | **Node.js 16+** / TypeScript |
| **Concurrency Model** | **Project Loom Virtual Threads** | Single-Threaded Event Loop |
| **I/O Blocking Mechanics** | Virtual threads unmount from carrier threads on blocking I/O | Non-blocking async/await promises |
| **Multi-Core CPU Scaling** | Natively concurrent across all JVM CPU cores | Requires spawning child OS processes ("sandboxed workers") |
| **Redis Lua Scripts** | **Direct execution of 49 official BullMQ Lua scripts** | **Official BullMQ Lua scripts** |
| **Redis Key Topology** | Standard `bull:<queue>:*` hierarchy | Standard `bull:<queue>:*` hierarchy |
| **Job Lifecycle States** | WAITING, ACTIVE, DELAYED, WAITING_CHILDREN, COMPLETED, FAILED, PAUSED, STALLED | WAITING, ACTIVE, DELAYED, WAITING_CHILDREN, COMPLETED, FAILED, PAUSED, STALLED |
| **Parent-Child DAG Workflows** | Built-in `FlowProducer` | Built-in `FlowProducer` |
| **Sliding-Window Rate Limiting**| Built-in token bucket with `groupKey` | Built-in token bucket with `groupKey` |
| **Job Deduplication & Debounce** | Built-in (`jobId` & `deduplicationId` windows) | Built-in (`jobId` & deduplication) |
| **Live Progress & Step Logs** | Built-in (`job.updateProgress`, `job.log`) | Built-in (`job.updateProgress`, `job.log`) |
| **Web Dashboard** | Native **Bull-Board UI** compatibility | Native **Bull-Board UI** compatibility |
| **Framework Integration** | **Spring Boot 3+ Starter** (`@OxmqListener`, Actuator) | Express, Fastify, NestJS BullMQ integration |
| **License** | **Apache 2.0** | **MIT** |

---

## 🔍 In-Depth Architectural Analysis

### 1. Concurrency: Project Loom Virtual Threads vs. Node.js Event Loop
* **BullMQ in Node.js**:
  - Operates on Node.js's single-threaded event loop.
  - Excellent for high-volume non-blocking asynchronous network I/O.
  - However, if tasks perform heavy CPU work (e.g. image processing, cryptography, PDF generation, large JSON parsing), the single thread can become blocked. BullMQ solves this by spawning child OS processes ("sandboxed workers"), which introduces inter-process communication (IPC) overhead.
* **OxMQ in Java 21**:
  - Operates on native **Project Loom Virtual Threads** (`Executors.newVirtualThreadPerTaskExecutor()`).
  - When tasks perform blocking calls (JDBC database queries, third-party REST calls, blocking socket I/O), the virtual thread unmounts from its underlying OS carrier thread with near-zero memory footprint (~1 KB per thread).
  - CPU-bound tasks execute concurrently across all available CPU cores on the JVM without needing separate OS child processes.

---

### 2. Polyglot Interoperability
* **Shared Redis Topology**:
  - OxMQ and BullMQ share the identical Redis key schema, sorted sets, lists, hashes, and streams.
  - This unlocks seamless cross-language workflows: a Node.js frontend can enqueue jobs into Redis with BullMQ, and an OxMQ worker in Java can process them with Virtual Threads, or vice versa.
* **Unified Dashboard**:
  - You can monitor both Java OxMQ queues and Node.js BullMQ queues side-by-side on the same **Bull-Board UI** instance.

---

## 🧪 Measuring Performance in Your Environment

Because throughput and latency depend heavily on network round-trip time (RTT), payload size, Redis persistence configuration (AOF vs RDB), and worker business logic, **we encourage running empirical benchmarks**:

OxMQ includes a dedicated **JMH (Java Microbenchmark Harness)** module in [`oxmq-benchmarks/`](https://github.com/gaurav10610/oxmq/tree/main/oxmq-benchmarks):

```bash
# Compile benchmark suite
./mvnw clean test-compile -pl oxmq-benchmarks

# Run benchmarks against your local or remote Redis instance
java -jar oxmq-benchmarks/target/oxmq-benchmarks-1.0.0-SNAPSHOT.jar
```

---

## 🙏 Attribution

OxMQ proudly stands on the shoulders of giants. We express our sincere appreciation to the open-source **[BullMQ](https://github.com/taskforcesh/bullmq)** project and its community for originating and maintaining world-class Redis queue architectures under the permissive MIT license. OxMQ directly reuses the official BullMQ Lua scripts to deliver 100% wire and functional parity in the Java ecosystem.
