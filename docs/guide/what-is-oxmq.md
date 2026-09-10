# What is OxMQ?

**OxMQ** is an ultra-high-performance distributed message and job queue engine for **Java 21+** engineered with native **Project Loom Virtual Threads** and offering **100% wire and functional parity with BullMQ v5**.

It bridges the distributed queue gap in the Java enterprise ecosystem, unlocking virtual thread concurrency while maintaining zero-protocol friction with Redis and BullMQ ecosystems (including [Bull-Board](https://github.com/felixmosh/bull-board)).

---

## 💡 The Problem: The Java Queue Dilemma

Historically, Java developers building distributed task processing faced an awkward compromise:

1. **Heavyweight Message Brokers (Kafka / RabbitMQ)**: Outstanding for event streaming and pub/sub routing, but complex to operate when you need atomic job delayed retries, job parent-child DAGs, progress tracking, rate limiting, and ad-hoc job cancellations.
2. **Traditional Java Redis Queues**: Built for Java 8/11 with fixed OS thread pools (`FixedThreadPool`). When tasks perform blocking I/O (database queries, third-party REST APIs, LLM calls), thread pools quickly become saturated, driving up memory consumption and latency.
3. **Node.js BullMQ Parity Gap**: Teams using BullMQ in TypeScript/Node had no direct equivalent in Java with matching Redis key topologies and Lua scripts, preventing seamless polyglot architectures.

---

## ⚡ The Solution: OxMQ

OxMQ resolves this by pairing **Redis atomic Lua scripts** with **Java 21 Virtual Threads**:

- **Native Loom Concurrency**: Every worker task executes on a lightweight Virtual Thread (`Executors.newVirtualThreadPerTaskExecutor()`). Virtual threads unmount from carrier threads during blocking I/O, allowing single worker nodes to comfortably handle thousands of concurrent in-flight jobs.
- **49 Official BullMQ Lua Scripts**: Direct execution of battle-tested BullMQ v5 scripts for atomic job state transitions, locking, scheduling, and rate limiting.
- **Full BullMQ Wire Parity**: Uses standard `{prefix}:{queue}:*` Redis keys and MessagePack serialization, allowing instant plug-and-play with **Bull-Board UI** and cross-language producers/workers.
- **Spring Boot 3+ Integration**: Declarative `@OxmqListener` annotations, auto-configuration, and Spring Boot Actuator health checks.

---

## 🏛️ High-Level Architecture

```text
+-------------------------------------------------------------------------+
|                           Application Layer                             |
|                                                                         |
|  +-------------------+  +--------------------+  +--------------------+  |
|  |   OxmqQueue<T>    |  |    FlowProducer    |  |   @OxmqListener    |  |
|  |    (Producer)     |  |  (DAG Workflows)   |  | (Spring Boot 3.3+) |  |
|  +---------+---------+  +---------+----------+  +---------+----------+  |
+------------|----------------------|-----------------------|-------------+
             |                      |                       |              
             v                      v                       v              
+-------------------------------------------------------------------------+
|                           OxMQ Core Engine                              |
|                                                                         |
|  +-------------------------------------------------------------------+  |
|  |           Project Loom Virtual Thread Task Dispatcher             |  |
|  |           (Executors.newVirtualThreadPerTaskExecutor())           |  |
|  +-------------------------------------------------------------------+  |
|                                                                         |
|  +------------------+  +-------------------+  +----------------------+  |
|  |  Lock Watchdog   |  |  Token Bucket     |  |  Micrometer Metrics  |  |
|  | & Auto-Extension |  |   Rate Limiter    |  |   & Redis Streams    |  |
|  +------------------+  +-------------------+  +----------------------+  |
+-----------------------------------|-------------------------------------+
                                    | EVALSHA
                                    v
+-------------------------------------------------------------------------+
|                  Redis Storage (BullMQ Wire-Compatible)                 |
|                                                                         |
|   bull:<queue>:wait      [FIFO List]       bull:<queue>:<id>   [Hash]   |
|   bull:<queue>:active    [Active List]     bull:<queue>:events [Stream] |
|   bull:<queue>:delayed   [Timestamp ZSet]  bull:<queue>:stalled[ZSet]   |
|   bull:<queue>:completed [TTL ZSet]        bull:<queue>:failed [ZSet]   |
+-------------------------------------------------------------------------+
```

---

## 🎯 Next Steps

- Check out the [Quickstart Guide](/guide/quickstart) to build your first queue and worker.
- Explore the [Spring Boot 3+ Starter](/guide/spring-boot) for declarative `@OxmqListener` usage.
- Read about [Project Loom Virtual Threads](/guide/virtual-threads) and concurrency management.
