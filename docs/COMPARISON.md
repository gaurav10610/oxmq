# ⚖️ Feature & Architecture Comparison
### *An Objective, Fact-Based Evaluation of OxMQ and Alternative Solutions*

Choosing the right background job processor or message engine is a foundational architectural decision. Different systems are optimized for different workload patterns:
* **Task Queues vs Event Streams:** Task queues manage individual job lifecycles (retries, timeouts, state transitions, progress tracking, and parent-child dependencies). Event streams (like Kafka) manage immutable, ordered logs of events.
* **In-Memory Redis vs Relational Databases:** In-memory sorted sets deliver sub-millisecond task dispatching without table-level database locks.
* **Virtual Threads vs Thread Pools:** Project Loom allows high-concurrency I/O without carrier thread pool exhaustion.

This document provides a factual, technically grounded comparison of **OxMQ** against leading solutions in the Java and polyglot ecosystems.

---

## 📊 Comprehensive Comparison Matrix

| Feature / Capability | 🐂 **OxMQ** (Java 21+) | 🐂 **BullMQ** (Node.js) | 💼 **JobRunr** (Java) | ⏱️ **Quartz / DB-Scheduler** | 🐰 **RabbitMQ** | 📨 **Apache Kafka** |
| :--- | :---: | :---: | :---: | :---: | :---: | :---: |
| **Primary Language** | **Java 21+** | Node.js / TypeScript | Java 11+ | Java 8+ | Erlang (Polyglot) | Java / Scala |
| **Concurrency Model** | **Virtual Threads (Loom)** | Event Loop (Single-thread) | Platform Threads (OS pool) | Platform Threads (OS pool) | Erlang Actor Processes | Thread-per-partition |
| **Primary Workload** | **Distributed Job Lifecycle** | Distributed Job Lifecycle | Background Job Processing | Scheduled Cron / Jobs | AMQP Message Routing | Event Streaming & Commit Log |
| **Parent-Child DAG Workflows** | ✅ **Built-in (Apache 2.0)** | ✅ Built-in | ❌ **JobRunr Pro Feature** | ❌ None | ❌ Manual client orchestration | ❌ External engine (Streams/Flink) |
| **Sliding-Window Rate Limiting**| ✅ **Built-in (Token Bucket)** | ✅ Built-in | ❌ **JobRunr Pro Feature** | ❌ None | ⚠️ Via plugin | ❌ Broker-level quotas only |
| **Batch Dequeue (Bulk Ingestion)**| ✅ **Atomic Bulk Lua Pop** | ⚠️ Partial | ❌ 1-by-1 processing | ❌ Batch poll, single dispatch | ⚠️ Prefetch only | ✅ Native batch polling |
| **Sub-Second Delays & Scheduling**| ✅ **Atomic Redis ZSET** | ✅ Atomic Redis ZSET | ⚠️ Periodic poll interval | ❌ Periodic DB poll (1–5s) | ⚠️ Dead-letter TTL / Plugin | ❌ Not supported natively |
| **Job Deduplication Windows** | ✅ **Built-in (`jobId` debounce)**| ✅ Built-in | ⚠️ Basic key uniqueness | ⚠️ Unique trigger keys | ❌ Manual tracking | ⚠️ Log Compaction / Idempotency |
| **Live Progress & Step Logs** | ✅ **Built-in (`job.updateProgress`)**| ✅ Built-in | ⚠️ Dashboard logs | ❌ None | ❌ None | ❌ None |
| **State Storage** | **Redis 6.2+ / 7.x / Valkey** | Redis / Valkey | SQL / MongoDB / Redis | Relational SQL (Postgres/MySQL) | Mnesia / Disk | Partitioned Commit Log |
| **Web Dashboard** | ✅ **Bull-Board UI (Native Parity)** | ✅ Bull-Board UI | ✅ JobRunr Dashboard | ❌ None (Third-party only) | ✅ RabbitMQ Management UI | ⚠️ Third-party (AKHQ/Kafdrop) |
| **License & Commercial Model** | **Apache 2.0 (100% Free)** | MIT | LGPLv3 / **Commercial Pro** | Apache 2.0 | MPL 2.0 | Apache 2.0 |

---

## 🔍 In-Depth Architectural Analysis

### 1. OxMQ vs JobRunr (Open Source vs JobRunr Pro)
* **Open Source Scope**:
  - **JobRunr** is a well-engineered Java background job library with a fluent lambda-based syntax and an embedded web dashboard.
  - However, several advanced enterprise capabilities—including **Parent-Child DAG Workflows**, **dynamic sliding-window rate limiting**, and **dynamic queue creation**—are strictly part of **JobRunr Pro** (a commercial offering requiring a paid subscription).
  - **OxMQ** provides full parent-child DAG hierarchies (`FlowProducer`), sliding-window token-bucket rate limiting, and dynamic queue creation **100% free under the Apache 2.0 open-source license**.
* **Concurrency Model**:
  - JobRunr uses traditional Java platform thread pools (typically sized to 20–50 OS threads). When jobs execute blocking I/O calls (such as external HTTP APIs or slow database queries), the pool can become saturated.
  - OxMQ uses **Java 21 Virtual Threads (Project Loom)**, allowing workers to execute thousands of concurrent tasks without carrier thread exhaustion or 1MB stack memory reservations.

---

### 2. OxMQ vs Quartz Scheduler & db-scheduler
* **Relational Database Polling vs In-Memory Sorted Sets**:
  - Relational schedulers (Quartz, db-scheduler) are great choices for simple applications that already have a relational database and do not want to manage Redis.
  - However, at scale, relational schedulers query database tables on a periodic polling interval (typically every 1 to 5 seconds) using locking queries (`SELECT ... FOR UPDATE`). This can introduce database lock contention, increased IOPS, and unavoidable polling latency.
  - OxMQ stores job states in **Redis in-memory data structures** (lists and sorted sets). Delay checks and job transitions execute via atomic Lua scripts in microseconds with zero relational database overhead.

---

### 3. OxMQ vs Apache Kafka & RabbitMQ
* **Message Broker vs Distributed Job Queue**:
  - **Apache Kafka** is a distributed append-only commit log designed for high-throughput sequential data streaming (event sourcing, clickstream analytics). It is intentionally not designed for individual job lifecycle management: Kafka has no native support for per-job delayed execution, individual job retry backoffs with custom delays, step progress tracking, or parent-child DAG resolution without an external stream processing system.
  - **RabbitMQ** is an AMQP message broker optimized for complex message routing across exchanges and queues. While it supports basic worker pools, it lacks native DAG workflow trees, sliding-window rate limiting, job progress streaming, and out-of-the-box job payload inspection without external plugins.
  - **OxMQ** is explicitly a **Distributed Job Queue & Workflow Engine**: it tracks discrete job states (`WAITING`, `ACTIVE`, `WAITING_CHILDREN`, `COMPLETED`, `FAILED`), exposes real-time percentage progress, records per-step execution logs, and automates retry backoffs.

---

### 4. OxMQ vs BullMQ (Node.js)
* **Wire Compatibility**:
  - OxMQ implements the exact Redis data schema used by BullMQ v5. This enables seamless **polyglot microservice architectures**: a Node.js API can enqueue jobs that an OxMQ Java worker consumes, or vice versa.
  - OxMQ works out-of-the-box with the standard **[Bull-Board Web UI](https://github.com/felixmosh/bull-board)**.
* **Virtual Threads vs Node.js Event Loop**:
  - BullMQ operates within Node.js's single-threaded event loop, which can become CPU-bound during intensive serialization, encryption, or computation.
  - OxMQ pairs the battle-tested BullMQ Redis protocol with Java 21's Virtual Threads and multi-core JVM performance.

---

## 🧪 Measuring Performance in Your Environment

Because throughput and latency depend significantly on network round-trip latency, payload size, Redis persistence configuration (AOF vs RDB), and worker business logic, **we encourage running empirical benchmarks**:

OxMQ includes a dedicated **JMH (Java Microbenchmark Harness)** module in [`oxmq-benchmarks/`](../oxmq-benchmarks):
```bash
mvn clean test-compile -pl oxmq-benchmarks
# Run benchmarks against your local or remote Redis instance:
java -jar oxmq-benchmarks/target/oxmq-benchmarks-1.0.0-SNAPSHOT.jar
```

---

## 🙏 Attribution

OxMQ proudly stands on the shoulders of giants. We express our sincere appreciation to the open-source **[BullMQ](https://github.com/taskforcesh/bullmq)** project and its community for originating and maintaining world-class Redis queue architectures under the permissive MIT license. OxMQ directly reuses the official BullMQ Lua scripts to deliver 100% wire and functional parity in the Java ecosystem.

