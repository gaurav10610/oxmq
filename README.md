<p align="center">
  <img src="docs/assets/oxmq-icon.png" alt="OxMQ Logo" width="140" style="border-radius: 20px;">
</p>

<h1 align="center" style="font-size: 2.6rem; font-weight: 800; margin-top: 0.5rem; margin-bottom: 0.5rem;">OxMQ</h1>

<p align="center">
  <b>The Enterprise Distributed Job Queue &amp; DAG Workflow Engine for Java 17+ &amp; Java 21 Loom</b><br/>
  <i>The BullMQ of Java • 100% Free &amp; Open Source (Apache 2.0) • Project Loom Virtual Threads • Batch Dequeue • Spring Boot 3 Starter</i>
</p>

<p align="center">
  <a href="https://gaurav10610.github.io/oxmq/"><img src="https://img.shields.io/badge/Documentation-GitHub%20Pages-blue.svg" alt="Documentation"></a>
  <a href="https://central.sonatype.com/artifact/io.github.gaurav10610/oxmq-core"><img src="https://img.shields.io/badge/Maven%20Central-v1.8.0-C71A36.svg" alt="Maven Central v1.8.0"></a>
  <a href="https://github.com/gaurav10610/oxmq/releases/tag/v1.8.0"><img src="https://img.shields.io/badge/Release-v1.8.0-blue.svg" alt="Release v1.8.0"></a>
  <a href="https://opensource.org/licenses/Apache-2.0"><img src="https://img.shields.io/badge/License-Apache%202.0-blue.svg" alt="License"></a>
  <a href="https://openjdk.org/projects/jdk/17/"><img src="https://img.shields.io/badge/Java-17%2B%20%7C%2021%2B%20LTS-orange.svg" alt="Java 17+ and 21+ LTS"></a>
  <a href="https://spring.io/projects/spring-boot"><img src="https://img.shields.io/badge/Spring%20Boot-3.x-6DB33F.svg" alt="Spring Boot 3"></a>
  <a href="https://bullmq.io/"><img src="https://img.shields.io/badge/BullMQ-100%25%20Wire%20Compatible-magenta.svg" alt="BullMQ Compatible"></a>
</p>

<p align="center">
  <a href="https://gaurav10610.github.io/oxmq/"><b>📚 Read the Official Documentation</b></a> •
  <a href="https://gaurav10610.github.io/oxmq/guide/quickstart"><b>⚡ Quickstart</b></a> •
  <a href="https://gaurav10610.github.io/oxmq/guide/spring-boot"><b>🍃 Spring Boot Guide</b></a> •
  <a href="https://gaurav10610.github.io/oxmq/ARCHITECTURE"><b>🏛️ Architecture &amp; Internals</b></a>
</p>

---

## Overview

**OxMQ** brings the gold standard of [BullMQ](https://github.com/taskforcesh/bullmq) to the modern **Java 17+ and Java 21+ (Project Loom)** ecosystem.

By directly executing BullMQ's official 49 Lua scripts and adhering to canonical Redis key topologies, OxMQ delivers **100% wire-format and script compatibility** with BullMQ while unlocking the massive concurrency of Java 21 Virtual Threads, enterprise multi-tenant context isolation, and high-throughput batch dequeuing.

## Key Capabilities

- **Project Loom Virtual Threads**: Execute **10,000+ concurrent I/O-bound workers** per JVM node with ~1 KB heap per task and zero OS carrier thread starvation (with automatic platform thread pool fallback on Java 17 LTS).
- **Virtual Queue Groups & Intra-Group Priority**: Fine-grained multi-tenant isolation with $O(1)$ fair circular round-robin scheduling across virtual groups, intra-group priority ordering (`group.priority`), dynamic per-group concurrency overrides (`groups:concurrency`), bounded backlogs (`maxSize`), and cluster-wide dynamic throttling (all **100% Free & Open-Source, NO Pro Tier or paywall**).
- **Cluster-Wide Distributed Job Cancellation**: Instantly halt running Virtual Threads across any Kubernetes worker pod via Redis Pub/Sub control channel (`queue.cancelJob(id, reason)`).
- **Resumable State Machines & Job Checkpointing**: Save execution state with `job.checkpoint(step, state)` to skip completed steps when retrying after a crash or network interruption.
- **Dynamic Runtime Child Spawning in DAGs**: Dynamically branch and spawn child tasks during execution (`job.addChild` / `job.addChildren`), pausing parent Virtual Threads in `WAITING_CHILDREN` and auto-resuming upon completion.
- **Declarative Per-Job-Type Timeouts**: Enforce distinct watchdog execution timeouts per job type (`jobTimeout(taskName, Duration)` and Spring Boot `@JobTimeout`).
- **Distributed Pub/Sub Control Plane & Relay**: High-throughput inter-node messaging (`OxmqRelay`) with hierarchical wildcard pattern matching (`*` and `>`), retained messages with TTL, and heartbeat node leases.
- **100% BullMQ Wire & Script Parity**: Executes BullMQ's official 49 Lua scripts directly inside Redis. Seamless polyglot interoperability (Java &harr; Node.js) and instant compatibility with the [Bull-Board Web UI](https://github.com/felixmosh/bull-board).
- **High-Throughput Batch Dequeue & Granular Partial Failures**: Bulk pop up to $N$ jobs atomically in a single Redis call (`OxmqBatchWorker`) with `minBatchSize` accumulation buffering, memory safety budgets (`maxBatchBytes`), homogeneous tenant batches (`groupAffinity`), and item-level partial failures via `job.setAsFailed(err)`.
- **In-Flight Hard Execution Timeouts**: Dedicated watchdog timer interrupts hanging virtual threads on timeout (`jobTimeout(Duration)`) and safely marks jobs failed in Redis with `JobTimeoutException`.
- **Transactional Outbox Pattern**: Zero message loss across relational databases (PostgreSQL, MySQL, Oracle, H2) and Redis in Spring Boot via `@EnableOxmqOutbox` and `OxmqOutboxService`.
- **Context Isolation & Multi-Tenancy**: Automatic service-level namespacing with strict 7-tier precedence and automated Redis Cluster single-slot hash-tag routing (`{prefix:queue}`).
- **Parent-Child DAG Workflows**: Orchestrate multi-tier dependency trees with automatic leaf deduplication and lazy result aggregation (`FlowProducer`).
- **Cluster Rate Limiting & Concurrency**: Native queue-level metadata (`setGlobalConcurrency`, `setGlobalRateLimit`, `isMaxed`) plus opt-in multi-tenant group limiter plugins.
- **Enterprise Spring Boot 3 Starter**: Declarative `@EnableOxmq`, `@OxmqListener`, `@OxmqBatchListener`, and `@EnableOxmqOutbox` annotations with Actuator health indicators.
- **OpenTelemetry Distributed Tracing**: Native W3C `traceparent` and `tracestate` context propagation across queues, workers, and DAG hierarchies.

---

## Quickstart

### 1. Add Maven Dependency

```xml
<dependency>
    <groupId>io.github.gaurav10610</groupId>
    <artifactId>oxmq-core</artifactId>
    <version>1.8.0</version>
</dependency>
```

*(For Spring Boot 3, add `oxmq-spring-boot-starter` instead).*

### 2. Standalone Java Example

```java
import io.oxmq.OxmqQueue;
import io.oxmq.OxmqWorker;
import io.oxmq.model.JobOptions;
import io.oxmq.model.KeepJobs;
import java.time.Duration;

// 1. Initialize Queue & Worker
OxmqQueue<OrderData> queue = OxmqQueue.<OrderData>builder()
        .name("orders")
        .redisUri("redis://localhost:6379")
        .payloadClass(OrderData.class)
        .build();

OxmqWorker<OrderData> worker = OxmqWorker.<OrderData>builder()
        .queueName("orders")
        .redisUri("redis://localhost:6379")
        .concurrency(100) // 100 Virtual Threads on Java 21+
        .processor(job -> processPayment(job.getData()))
        .build();
worker.start();

// 2. Enqueue Job with Retries and Exponential Backoff
queue.add("process-order", new OrderData("ord_101", 99.00), JobOptions.builder()
        .attempts(3)
        .exponentialBackoff(Duration.ofSeconds(1), Duration.ofSeconds(30))
        .removeOnComplete(KeepJobs.count(1000))
        .build());
```

### 3. Spring Boot 3 Declarative Listener

```java
@Component
public class PaymentOrderListener {

    @OxmqListener(queue = "orders", concurrency = 50)
    public void handleOrder(Job<OrderData> job) {
        OrderData order = job.getData();
        paymentGateway.charge(order);
    }
}
```

---

## Architectural Comparison: OxMQ vs. BullMQ

| Capability | OxMQ (Java 17 / 21) | BullMQ (Node.js) |
| :--- | :--- | :--- |
| **Runtime Target** | Java 17+ LTS / Java 21+ Project Loom | Node.js 16+ / TypeScript |
| **Concurrency Engine** | Project Loom Virtual Threads | Single-Threaded Event Loop |
| **Virtual Queue Groups** | **Built-in ($O(1)$ fair circular round-robin)** | **Commercial Paywall (BullMQ Pro license)** |
| **Group Concurrency & Rate Limiting** | **Built-in (`groupConcurrency`, `setGroupRateLimit`, `getGroupRateLimitTtl`)** | **Commercial Paywall (BullMQ Pro)** |
| **Global Across-Groups Breakdown & Repair** | **Built-in (`getGroups`, `getGroupsJobsCount`, `repairGroup`, `repairMaxedGroups`)** | **Commercial Paywall (BullMQ Pro)** |
| **Reactive Stream Processors & Multi-Stage Checkpoints** | **Built-in (`reactiveProcessor`, `Flux`, `Mono`, `Flow.Publisher`)** | **Commercial Paywall (BullMQ Pro)** |
| **Spring Boot Actuator Group REST Endpoints** | **Built-in (`/actuator/oxmq-groups` GET & POST management)** | None / Custom API development |
| **Batch Worker with Group Affinity** | **Built-in (homogeneous tenant batches)** | **Commercial Paywall (BullMQ Pro)** |
| **Item-Level Batch Partial Failure** | **Built-in (`job.setAsFailed(err)`)** | **Commercial Paywall (BullMQ Pro)** |
| **In-Flight Hard Timeouts** | **Built-in (`jobTimeout(Duration)`)** | Manual timer wrappers |
| **Transactional Outbox Engine** | **Built-in (`@EnableOxmqOutbox`, `OxmqOutboxService`)** | Requires external CDC (Debezium) |
| **Batch Dequeue** | Native atomic batch dequeue (`OxmqBatchWorker`) | Single-job polling only |
| **Multi-Tenancy** | Automatic service-level namespace inheritance | Manual per-queue prefix |
| **Redis Lua Scripts** | Direct execution of 49 official BullMQ scripts | Official BullMQ scripts |
| **Queue Range Pagination** | Built-in `getJobs`, `getWaiting`, `getActive`, etc. | Built-in `getJobs` |
| **Global Concurrency & Limits** | Built-in `setGlobalConcurrency`, `setGlobalRateLimit` | Built-in `setGlobalConcurrency`, `setGlobalRateLimit` |
| **Custom Backoff Registry** | Dynamic `BackoffStrategy.custom(...)` + worker registry | Custom backoff functions |
| **Distributed Tracing** | Native W3C `TraceContext` (`traceparent`, `tracestate`) | OpenTelemetry tracer opts |
| **Web Dashboard** | Native Bull-Board UI compatibility | Native Bull-Board UI compatibility |
| **Pricing & Pro Tier** | **100% Free & Open Source (NO PRO TIER)** | **Commercial BullMQ Pro (annual subscription)** |
| **License** | Apache 2.0 (100% Free & Open Source) | MIT (Community) / Commercial (Pro) |

---

## Documentation Directory

For complete documentation, in-depth architectural specifications, and operational runbooks, visit our **[Official Documentation Site](https://gaurav10610.github.io/oxmq/)**:

### Getting Started
- **[What is OxMQ?](https://gaurav10610.github.io/oxmq/guide/what-is-oxmq)**: Overview and core design goals.
- **[Quickstart Guide](https://gaurav10610.github.io/oxmq/guide/quickstart)**: Setup and first job execution.
- **[Spring Boot 3 Starter Guide](https://gaurav10610.github.io/oxmq/guide/spring-boot)**: Declarative `@OxmqListener` and auto-configuration.
- **[Project Loom Virtual Threads](https://gaurav10610.github.io/oxmq/guide/virtual-threads)**: Concurrency model and thread unmounting.
- **[Context Isolation & Namespacing](https://gaurav10610.github.io/oxmq/guide/namespacing)**: 7-tier multi-tenant precedence.
- **[Configuration Reference](https://gaurav10610.github.io/oxmq/guide/configuration)**: Full YAML and builder reference.

### Core Concepts & Patterns
- **[Virtual Queue Groups & Multi-Tenancy](https://gaurav10610.github.io/oxmq/patterns/virtual-queue-groups)**: Fair round-robin scheduling, concurrency caps, and dynamic throttling.
- **[Transactional Outbox Pattern](https://gaurav10610.github.io/oxmq/patterns/transactional-outbox)**: Zero message loss across RDBMS and Redis in Spring Boot.
- **[Queues & Batching](https://gaurav10610.github.io/oxmq/concepts/queues)**: Queue management, range pagination, and global concurrency.
- **[Jobs & Lifecycle](https://gaurav10610.github.io/oxmq/concepts/jobs)**: States, `jobTimeout`, progress, and logs.
- **[Workers & Concurrency](https://gaurav10610.github.io/oxmq/concepts/workers)**: Concurrency, event emitters, and cancellation tokens.
- **[FlowProducer (DAG Workflows)](https://gaurav10610.github.io/oxmq/concepts/flows)**: Parent-child task trees and result aggregation.
- **[Rate Limiting & Concurrency](https://gaurav10610.github.io/oxmq/patterns/rate-limiting)**: Global queue metadata and multi-tenant limiter plugins.
- **[Retries & Backoff Policies](https://gaurav10610.github.io/oxmq/patterns/retries-backoff)**: Exponential, fixed, and custom backoff strategies.
- **[High-Throughput Batch Dequeue](https://gaurav10610.github.io/oxmq/patterns/batch-dequeue)**: Granular partial failures, accumulation buffering, and group affinity.

### Architecture & Operations
- **[Architecture & Engine Internals](https://gaurav10610.github.io/oxmq/ARCHITECTURE)**: Hash field dictionary, sequence diagrams, and Redis Cluster slot safety.
- **[49 Official Lua Scripts Catalog](https://gaurav10610.github.io/oxmq/operations/lua-scripts-catalog)**: Complete command inventory.
- **[Redis Key Topology & Wire Format](https://gaurav10610.github.io/oxmq/operations/redis-topology)**: Key structure and data structures.
- **[Memory Optimization & Tuning](https://gaurav10610.github.io/oxmq/operations/memory-optimization)**: Zero-allocation poller and stack trace caps.
- **[Bull-Board Web UI Dashboard](https://gaurav10610.github.io/oxmq/interop/bull-board)**: Standalone UI setup with zero Node.js code.
- **[Scale & DAG Benchmarks](https://gaurav10610.github.io/oxmq/operations/benchmarks)**: Profiling up to 10,000 DAG nodes and 3,000+ jobs/s.
- **[Disaster Recovery & Chaos Resilience](https://gaurav10610.github.io/oxmq/operations/disaster-recovery)**: Reconnection, checkpoints, and Kubernetes termination hooks.
- **[Production Troubleshooting](https://gaurav10610.github.io/oxmq/operations/troubleshooting)**: Diagnostic runbooks and solutions.

### Enterprise Showcase
- **[OxDoc 365 (Enterprise Document AI Pipeline)](https://gaurav10610.github.io/oxmq/guide/oxdoc-365)**: 5-tier multi-tenant SaaS document intelligence showcase modeling Microsoft Graph RU quotas, AIMD adaptive rate limiting, and DAG extraction pipelines.
- **[Multi-Tenant Dynamic Scaling Strategy](https://gaurav10610.github.io/oxmq/operations/oxdoc-365-multi-tenant-strategy)**: Architecture for 5 concurrent tenant tiers and zero-downtime chaos resilience.

---

## License & Attribution

OxMQ is licensed under the [Apache License 2.0](LICENSE).  
Official Redis Lua scripts adapted directly from the open-source [BullMQ](https://github.com/taskforcesh/bullmq) project under the permissive [MIT License](oxmq-core/src/main/resources/lua/BULLMQ_ATTRIBUTION.md).

### Author & Maintainer
Created and maintained by **[Gaurav Kumar Yadav](https://www.linkedin.com/in/gaurav-kumar-yadav-6125817a/)** ([GitHub: @gaurav10610](https://github.com/gaurav10610)). Contributions, discussions, and benchmark feedback are warmly welcome!
