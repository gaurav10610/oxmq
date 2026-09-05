# 🐂 OxMQ

> **High-Performance, Virtual Thread-Native Distributed Job Queue & DAG Workflow Engine for Java 21+**  
> *100% Open Source • BullMQ Wire-Compatible • Native Micrometer Telemetry • Zero-Config Spring Boot Starter*

[![License](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](https://opensource.org/licenses/Apache-2.0)
[![Java](https://img.shields.io/badge/Java-21%2B%20LTS-orange.svg)](https://openjdk.org/projects/jdk/21/)
[![Redis](https://img.shields.io/badge/Redis-6.2%2B%20%7C%207.x-red.svg)](https://redis.io)
[![Loom](https://img.shields.io/badge/Concurrency-Virtual%20Threads-brightgreen.svg)](https://openjdk.org/projects/loom/)
[![BullMQ Compatible](https://img.shields.io/badge/BullMQ-Wire%20Compatible-blueviolet.svg)](https://bullmq.io/)

---

## ⚡ Why OxMQ?

* **🚀 Virtual Thread Native (Project Loom):** Effortlessly run 1,000+ to 10,000+ concurrent I/O-bound workers (HTTP calls, DB queries, LLMs) on a single JVM node with minimal memory footprint.
* **💯 100% Free & Open Source:** Full support for Parent-Child DAG Workflows, Token-Bucket Rate Limiting, Dynamic Queues, and Sub-second Delays with zero paywalls (unlike JobRunr Pro).
* **🌐 BullMQ Wire-Compatible:** Shared Redis key layout and Lua scripts allow Java, Node.js, and Python microservices to produce and consume jobs across the same Redis cluster, with instant compatibility with [Bull-Board](https://github.com/felixmosh/bull-board).
* **📊 Native Performance Telemetry:** Built-in Micrometer instrumentation exposing counters, gauges, and high-precision latency distribution timers (`p50`, `p95`, `p99`) out-of-the-box for Prometheus and Grafana.
* **✨ Minimal Setup & Ergonomics:** Start producing and consuming jobs in under 5 lines of code.

---

## 📦 Architecture Overview

```
+-----------------------------------------------------------------------------------+
|                           User Application Layer                                 |
|             (Spring Boot / Quarkus / Micronaut / Standalone Java)                 |
+-----------------------------------------------------------------------------------+
                                         │
                 ┌───────────────────────┴───────────────────────┐
                 ▼                                               ▼
+----------------------------------+            +----------------------------------+
|      oxmq-spring-boot-starter    |            |            oxmq-core             |
|  - @EnableOxmq                   |            |  - Queue<T> & Worker<T>          |
|  - @OxmqListener annotations     |            |  - FlowProducer (DAG Engine)     |
|  - Spring Data Redis reuse       |            |  - Virtual Thread Dispatcher     |
|  - Actuator Endpoint & Health    |            |  - Stalled Job Watchdog          |
|  - OxmqProperties auto-bind      |            |  - OxmqMetrics (Micrometer)      |
+----------------------------------+            +----------------------------------+
                 │                                               │
                 └───────────────────────┬───────────────────────┘
                                         ▼
+-----------------------------------------------------------------------------------+
|                                  Engine Layer                                     |
|  - Redis Driver   : Lettuce 6.x (Async, Reactive, Sentinel, Cluster native)       |
|  - Serialization  : Pluggable (Jackson 2.x default with JavaTime & Records)       |
|  - Telemetry      : Native Micrometer (Prometheus, Grafana, OpenTelemetry)        |
|  - Lua Engine     : BullMQ v5 Atomic Lua Scripts (addJob, moveToActive, etc.)     |
+-----------------------------------------------------------------------------------+
```

---

## 🚀 Quickstart (Standalone Java 21 in 5 Lines)

### 1. Add Dependency

```xml
<dependency>
    <groupId>io.oxmq</groupId>
    <artifactId>oxmq-core</artifactId>
    <version>1.0.0-SNAPSHOT</version>
</dependency>
```

### 2. Produce a Job

```java
import io.oxmq.OxmqQueue;
import io.oxmq.model.JobOptions;
import java.time.Duration;

public record EmailNotification(String to, String subject, String body) {}

// Initialize Queue
OxmqQueue<EmailNotification> queue = OxmqQueue.<EmailNotification>builder()
    .name("notifications")
    .redisUri("redis://localhost:6379")
    .build();

// Enqueue with 5s delay, 3 retries, exponential backoff
queue.add("welcome-email", new EmailNotification("alice@example.com", "Welcome!", "Hello Alice!"),
    JobOptions.builder()
        .delay(Duration.ofSeconds(5))
        .attempts(3)
        .exponentialBackoff(Duration.ofSeconds(1))
        .build());
```

### 3. Consume with Virtual Threads

```java
import io.oxmq.OxmqWorker;

OxmqWorker<EmailNotification> worker = OxmqWorker.<EmailNotification>builder()
    .queueName("notifications")
    .redisUri("redis://localhost:6379")
    .concurrency(100) // 100 concurrent Virtual Threads!
    .processor(job -> {
        job.updateProgress(50);
        System.out.println("Sending email to " + job.getData().to());
        return "DELIVERED";
    })
    .build();

worker.start();
```

---

## 🍃 Spring Boot 3.x Starter

### 1. Add Starter Dependency

```xml
<dependency>
    <groupId>io.oxmq</groupId>
    <artifactId>oxmq-spring-boot-starter</artifactId>
    <version>1.0.0-SNAPSHOT</version>
</dependency>
```

### 2. Configure `application.yml`

```yaml
oxmq:
  redis:
    uri: redis://localhost:6379
  default-concurrency: 50
  metrics:
    enabled: true
```

### 3. Define `@OxmqListener`

```java
import io.oxmq.model.Job;
import io.oxmq.spring.annotation.EnableOxmq;
import io.oxmq.spring.annotation.OxmqListener;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.stereotype.Component;

@SpringBootApplication
@EnableOxmq
public class Application {
    public static void main(String[] args) {
        SpringApplication.run(Application.class, args);
    }
}

@Component
public class NotificationWorker {

    @OxmqListener(queue = "notifications", concurrency = 100)
    public String processNotification(Job<EmailNotification> job) {
        job.updateProgress(50);
        // Process webhook / email / LLM call in a Virtual Thread
        return "SUCCESS";
    }
}
```

---

## 🌳 Parent-Child DAG Workflows (`FlowProducer`)

```java
import io.oxmq.FlowProducer;
import io.oxmq.model.FlowJob;

FlowProducer flowProducer = new FlowProducer(redisClient);

// Parent waits for all children to complete
FlowJob<String> flow = FlowJob.of("video-encoder", "final-assembly-task")
    .addChild(FlowJob.of("video-encoder", "chunk-1-1080p"))
    .addChild(FlowJob.of("video-encoder", "chunk-2-1080p"))
    .addChild(FlowJob.of("video-encoder", "chunk-3-1080p"));

flowProducer.add(flow);
```

---

## 📊 Native Performance Telemetry (Micrometer)

OxMQ comes with first-class Micrometer metrics out-of-the-box:

* `oxmq.jobs.enqueued` (Counter)
* `oxmq.jobs.completed` (Counter)
* `oxmq.jobs.failed` (Counter)
* `oxmq.jobs.retried` (Counter)
* `oxmq.jobs.stalled` (Counter)
* `oxmq.jobs.active` (Gauge)
* `oxmq.jobs.waiting` (Gauge)
* `oxmq.jobs.delayed` (Gauge)
* `oxmq.job.duration` (Timer with p50, p95, p99 percentiles)
* `oxmq.job.wait_time` (Timer)

---

## 🖥️ Bull-Board UI Integration

Because OxMQ uses BullMQ's standard Redis schema, you can run Bull-Board with zero extra configuration:

```bash
npx @bull-board/cli --redis redis://localhost:6379 --queues notifications,video-encoder
```

Navigate to `http://localhost:3000` to inspect queues, active jobs, retry failures, and view logs!

---

## 📚 Project Documentation

* 📖 **[PRD (Product Requirements Document)](docs/PRD.md)**
* 🗺️ **[Roadmap](docs/ROADMAP.md)**
* 📋 **[Action Plan](docs/ACTION_PLAN.md)**
* 🏛️ **[Architecture & Internals](docs/ARCHITECTURE.md)**
* ⏱️ **[Benchmarks & Performance](docs/BENCHMARKS.md)**
* 📊 **[Executive Progress Tracker](PROGRESS_TRACKER.md)**

---

## 📄 License

OxMQ is licensed under the [Apache License 2.0](LICENSE).
