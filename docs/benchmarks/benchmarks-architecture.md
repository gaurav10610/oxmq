# Benchmarks & Architecture Evaluation

This document presents an objective, fact-based engineering evaluation of **OxMQ**, including its architectural design, comparative trade-offs with alternative frameworks, and reproducible JMH benchmark instructions.

---

## 📊 Feature & Architecture Comparison

| Feature / Capability | 🐂 **OxMQ** (Java 21+) | 🐂 **BullMQ** (Node.js) | 💼 **JobRunr** (Java) | ⏱️ **Quartz / db-scheduler** | 📨 **Apache Kafka** |
| :--- | :---: | :---: | :---: | :---: | :---: |
| **Primary Language** | **Java 21+** | TypeScript / Node.js | Java 11+ | Java 8+ | Java / Scala |
| **Concurrency Model** | **Virtual Threads (Loom)** | Event Loop | Platform OS Pool | Platform OS Pool | Thread-per-partition |
| **State Storage** | **Redis 6.2+ / Valkey** | Redis / Valkey | SQL / Mongo / Redis | Relational SQL | Partitioned Commit Log |
| **Parent-Child DAG Workflows** | ✅ **Built-in (Apache 2.0)** | ✅ Built-in | ❌ **JobRunr Pro Feature** | ❌ None | ❌ Requires Flink/Streams |
| **Sliding-Window Rate Limiting** | ✅ **Built-in (Token Bucket)** | ✅ Built-in | ❌ **JobRunr Pro Feature** | ❌ None | ❌ Broker quotas only |
| **Sub-Second Delays & Scheduling** | ✅ **Atomic Redis ZSet** | ✅ Atomic Redis ZSet | ⚠️ Periodic poll | ❌ Periodic DB poll (1-5s) | ❌ Not supported natively |
| **Polyglot Wire Compatibility** | ✅ **BullMQ v5 Wire Match** | ✅ Native | ❌ Java only | ❌ Java only | ✅ Client SDKs |
| **Web Dashboard** | ✅ **Bull-Board UI** | ✅ Bull-Board UI | ✅ Embedded UI | ❌ Third-party only | ⚠️ Third-party |
| **License** | **Apache 2.0 (100% Free)** | MIT | LGPLv3 / Commercial Pro | Apache 2.0 | Apache 2.0 |

---

## 🔬 Architectural Analysis

### OxMQ vs. Relational Schedulers (Quartz, db-scheduler)
- **Relational Polling**: Traditional database schedulers query relational tables on periodic intervals (1–5 seconds) using locking queries (`SELECT ... FOR UPDATE`). This can introduce database lock contention and latency under high volume.
- **In-Memory Redis ZSets**: OxMQ maintains delayed and waiting queues entirely in Redis in-memory data structures. Transitions execute in microseconds via atomic Lua scripts with zero relational database load.

### OxMQ vs. Event Brokers (Kafka, RabbitMQ)
- **Log Streaming vs. Task Lifecycle**: Kafka is designed for immutable, sequential event streaming. It intentionally lacks per-job delayed retries, individual retry backoffs, step progress tracking, or parent-child DAG resolution without external stream processing systems.
- **Discrete Job State Machine**: OxMQ manages individual job states (`WAITING`, `ACTIVE`, `WAITING_CHILDREN`, `COMPLETED`, `FAILED`), exposes real-time percentage progress, and tracks step execution logs.

---

## 🧪 Reproducible JMH Benchmarks

OxMQ includes a dedicated **JMH (Java Microbenchmark Harness)** module in `oxmq-benchmarks/` to measure producer and consumer throughput in your specific environment:

```bash
# Compile benchmark suite
./mvnw clean test-compile -pl oxmq-benchmarks

# Run JMH benchmark against local or remote Redis
java -jar oxmq-benchmarks/target/oxmq-benchmarks-1.0.0.jar
```

### Key Performance Characteristics:
- **Atomic Lua Transitions**: Single network round-trip per state transition.
- **Lightweight Threads**: Virtual threads unmount during network delays, allowing thousands of concurrent workers per node with minimal RAM footprint (~1 KB per thread vs 1 MB for platform threads).
- **Pipelined Bulk Enqueue**: `queue.addBulk()` leverages Redis pipelining for high-throughput task insertion.
