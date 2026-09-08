# ⚖️ Feature & Architecture Comparison
### *Why OxMQ is the Definitive Distributed Job Queue for Java 21*

Choosing the right background job processor or message engine is a foundational architectural decision. Every major programming ecosystem has a clear standard for high-performance distributed background jobs:
* **Python:** Celery
* **Node.js:** BullMQ
* **Go:** Asynq
* **Java:** Historically fragmented between heavy message streams (Kafka/RabbitMQ), slow database pollers (Quartz), and commercial paywalls (JobRunr Pro).

**OxMQ fills this critical gap natively for Java 21+ and Spring Boot 3.**

---

## 📊 Comprehensive Comparison Matrix

| Feature / Capability | 🐂 **OxMQ** (Java 21+) | 🐂 **BullMQ** (Node.js) | 💼 **JobRunr** (Java) | ⏱️ **Quartz / DB-Scheduler** | 🐰 **RabbitMQ** | 📨 **Apache Kafka** |
| :--- | :---: | :---: | :---: | :---: | :---: | :---: |
| **Primary Language** | **Java 21+** | Node.js / TypeScript | Java 11+ | Java 8+ | Erlang (Polyglot) | Java / Scala |
| **Concurrency Model** | **Virtual Threads (Loom)** | Event Loop (Single-thread) | Platform Threads (OS pool) | Platform Threads (OS pool) | Erlang Actors | Thread-per-partition |
| **Parent-Child DAGs** | ✅ **100% Free (Apache 2.0)** | ✅ Free | ❌ **Paywalled ($99+/mo)** | ❌ None | ❌ Manual orchestration | ❌ Requires Flink/Streams |
| **Sliding-Window Rate Limiting**| ✅ **Built-in** | ✅ Built-in | ❌ **Paywalled** | ❌ None | ⚠️ Plugin required | ❌ None |
| **Batch Dequeue (Bulk Ingestion)**| ✅ **Built-in (>= 50k ops/s)** | ⚠️ Partial | ❌ None | ❌ None | ⚠️ Prefetch only | ✅ Native batching |
| **Sub-Second Delays** | ✅ **Atomic ZSET (<1ms)** | ✅ Atomic ZSET | ⚠️ Polling lag | ❌ 1–5s DB poll lag | ⚠️ DLX plugin trick | ❌ None |
| **Job Deduplication Windows** | ✅ **Built-in (jobId)** | ✅ Built-in | ⚠️ Basic | ❌ Manual | ❌ Manual | ⚠️ Log Compaction |
| **Live Step Progress & Logs** | ✅ **Built-in API** | ✅ Built-in API | ⚠️ UI only | ❌ None | ❌ None | ❌ None |
| **State Storage** | **Redis 6.2+ / 7.x / Valkey** | Redis / Valkey | SQL / Mongo / Redis | SQL (Postgres, MySQL) | Erlang Mnesia / Disk | Append-Only Disk Log |
| **Web UI Dashboard** | ✅ **Bull-Board UI (Native)** | ✅ Bull-Board UI | ✅ JobRunr Dashboard | ❌ None | ✅ RabbitMQ Admin | ⚠️ Third-party (Kafdrop) |
| **Throughput (Ops/sec)** | **$\ge 25,000$ to $50,000+$** | $\sim 10,000$ ops/s | $\sim 2,500$ ops/s | $\sim 500$ ops/s | $\sim 50,000$ ops/s | $\ge 100,000$ ops/s |
| **License** | **Apache 2.0 (100% Open)** | MIT | LGPLv3 / **Commercial Pro** | Apache 2.0 | MPL 2.0 | Apache 2.0 |

---

## 🔍 In-Depth Architectural Breakdown

### 1. OxMQ vs JobRunr & JobRunr Pro
* **100% Free Open Source vs Expensive Commercial Paywalls**: JobRunr locks essential enterprise capabilities behind **JobRunr Pro** (costing thousands of dollars annually):
  - Parent-Child DAG workflows (`FlowProducer`)
  - Sliding-window rate limiting (`@RateLimit`)
  - Dynamic queue creation and batching
  - **OxMQ delivers all of these capabilities 100% free under the Apache 2.0 license.**
* **Virtual Threads vs OS Thread Starvation**: JobRunr runs tasks on traditional Java platform thread pools (typically 20–50 OS threads). When jobs execute blocking network calls (e.g. OpenAI API, Stripe webhooks, database writes), the entire pool starves. OxMQ runs on **Java 21 Project Loom**, enabling **1,000+ to 10,000+ concurrent Virtual Threads** per node with $< 2\text{KB}$ memory per task.

---

### 2. OxMQ vs Quartz & Relational DB Schedulers (db-scheduler)
* **Eliminates Database Polling & Table Locks**: Relational schedulers rely on periodic SQL queries (`SELECT ... FOR UPDATE WHERE execution_time <= NOW()`) executed every 1–5 seconds across clustered instances. This causes severe lock contention, high database CPU load, and up to 5-second trigger delays.
* **Sub-Millisecond Redis Execution**: OxMQ leverages atomic Redis sorted sets (`ZSET`) and Lua scripts. Delayed jobs evaluate and transition to active state in $< 1\text{ms}$ with zero database overhead.

---

### 3. OxMQ vs Apache Kafka & RabbitMQ
* **Job Queue vs Append-Only Event Log**:
  - **Apache Kafka** is a distributed append-only commit log designed for streaming high-volume sequential event streams (analytics, event sourcing). Kafka is **not** a job queue: it has no native concept of individual job retries with exponential backoff, per-job scheduling delays, step-by-step progress tracking, job deduplication windows, or dead-letter re-driving without building complex external state stores.
  - **RabbitMQ** is an AMQP message broker ideal for message routing, but lacks native DAG workflow trees, sliding-window rate limit token buckets, job progress reporting, and out-of-the-box management dashboards with job payload inspection.
  - **OxMQ** is a dedicated **Distributed Job Queue & DAG Workflow Engine** specifically optimized for discrete background task lifecycles, retries, rate limits, and hierarchical execution graphs.

---

### 4. OxMQ vs BullMQ (Node.js)
* **Wire Protocol Parity**: OxMQ utilizes the exact same Redis data layout as BullMQ v5. This enables a unified, polyglot architecture: a Node.js API service can enqueue jobs that a high-performance Java 21 OxMQ worker consumes, and vice versa.
* **Java 21 Loom vs Node.js Event Loop**: Node.js operates on a single-threaded event loop, which can become CPU-bound when parsing large JSON payloads, running encryption, or processing images. Java 21's Virtual Threads combined with multi-core JVM JIT compilation deliver higher sustained throughput, deterministic latency, and superior multi-core utilization.
