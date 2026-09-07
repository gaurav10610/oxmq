# ⚖️ Feature & Architecture Comparison

Choosing the right background job processor or message engine is a critical architectural decision. This document details where **OxMQ** excels compared to existing alternatives in the Java, Node.js, and Python ecosystems.

---

## 📊 Comprehensive Comparison Matrix

| Feature / Capability | 🐂 **OxMQ** (Java 21+) | 🐂 **BullMQ** (Node.js) | 💼 **JobRunr** (Java) | ⏱️ **Quartz / DB-Scheduler** | 🐰 **RabbitMQ** | 📨 **Apache Kafka** |
| :--- | :---: | :---: | :---: | :---: | :---: | :---: |
| **Primary Language** | **Java 21+** | Node.js / TS | Java 11+ | Java 8+ | Erlang (Polyglot) | Java / Scala |
| **Concurrency Model** | **Virtual Threads (Loom)** | Event Loop (Single-thread) | Platform Threads (OS pool) | Platform Threads (OS pool) | Erlang Actors | Thread-per-partition |
| **Parent-Child DAGs** | ✅ **100% Free** | ✅ Free | ❌ **Paywalled ($99+/mo)** | ❌ None | ❌ Manual | ❌ Manual |
| **Sliding-Window Rate Limiting**| ✅ **Built-in** | ✅ Built-in | ❌ **Paywalled** | ❌ None | ⚠️ Plugin | ❌ None |
| **Batch Dequeue (Bulk Ingestion)**| ✅ **Built-in** | ⚠️ Partial | ❌ None | ❌ None | ⚠️ Prefetch | ✅ Native batching |
| **Sub-Second Delays** | ✅ **Atomic ZSET** | ✅ Atomic ZSET | ⚠️ Polling lag | ❌ 1–5s DB poll lag | ⚠️ DLX trick | ❌ None |
| **Deduplication Windows** | ✅ **Built-in** | ✅ Built-in | ⚠️ Basic | ❌ Manual | ❌ Manual | ⚠️ Compaction |
| **State Storage** | **Redis / Valkey** | Redis / Valkey | SQL / Mongo / Redis | SQL (Postgres, MySQL) | Erlang Mnesia / Disk | Disk Commit Log |
| **Web UI Dashboard** | ✅ **Bull-Board UI** | ✅ Bull-Board UI | ✅ JobRunr Dashboard | ❌ None | ✅ RabbitMQ Admin | ⚠️ Third-party |
| **Throughput (Ops/sec)** | **$\ge 25,000$ ops/s** | $\sim 10,000$ ops/s | $\sim 2,500$ ops/s | $\sim 500$ ops/s | $\sim 50,000$ ops/s | $\ge 100,000$ ops/s |
| **License** | **Apache 2.0 (100% Open)**| MIT | LGPLv3 / **Commercial Pro** | Apache 2.0 | MPL 2.0 | Apache 2.0 |

---

## 🔍 In-Depth Architectural Breakdown

### 1. OxMQ vs JobRunr & JobRunr Pro
* **Free vs Commercial Paywalls**: JobRunr locks essential enterprise capabilities (complex workflows/DAGs, dynamic rate limiting, job batching, and dynamic queues) behind an expensive commercial license. OxMQ provides **all** of these features 100% free under Apache 2.0.
* **Virtual Threads vs Platform Thread Pools**: JobRunr's standard configuration relies on bounded platform thread pools (e.g. 20–50 threads). If tasks perform blocking HTTP or LLM API calls, threads starve quickly. OxMQ runs on **Java 21 Project Loom**, comfortably executing **1,000+ concurrent Virtual Threads** per node.

---

### 2. OxMQ vs Quartz / Relational DB Schedulers
* **Polling Latency**: Relational schedulers (Quartz, db-scheduler) query SQL tables periodically (every 1–5 seconds) with `SELECT ... FOR UPDATE`, causing database lock contention and delayed execution.
* **Sub-Millisecond Execution**: OxMQ leverages Redis sorted sets (`ZSET`) and Lua scripts, evaluating delayed jobs and transitions in $< 1\text{ms}$.

---

### 3. OxMQ vs RabbitMQ & Apache Kafka
* **Message Broker vs Distributed Job Orchestrator**:
  - **Kafka** is a distributed append-only commit log designed for high-throughput sequential data streams (analytics, event sourcing). It is not designed for individual job retries, complex DAG trees, delayed job scheduling, or interactive progress reporting.
  - **RabbitMQ** is an AMQP broker great for routing messages, but lacks native DAG dependency resolution, sliding-window rate limit token buckets, and step-by-step job progress tracking.
  - **OxMQ** is a dedicated **Job Queue & Workflow Engine** designed for discrete task lifecycles, retries, exponential backoffs, rate limits, and multi-stage DAGs.

---

### 4. OxMQ vs BullMQ (Node.js)
* **Polyglot Interoperability**: OxMQ uses BullMQ's exact wire schema in Redis. A Node.js service can enqueue a job that a Java 21 OxMQ worker consumes, or vice versa.
* **Java Virtual Thread Performance**: In CPU/memory-bound mixed workloads, Java 21 Virtual Threads deliver higher raw throughput and lower latency variance compared to Node.js's single-threaded event loop.
