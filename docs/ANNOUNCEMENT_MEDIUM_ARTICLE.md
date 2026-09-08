# Introducing OxMQ: The Virtual Thread-Native Distributed Job Queue for Java 21 & Redis

### *How Java 21’s Project Loom, atomic Redis Lua state transitions, and BullMQ wire compatibility are redefining background task orchestration for modern JVM microservices.*

---

<p align="center">
  <img src="https://raw.githubusercontent.com/gaurav10610/oxmq/main/docs/assets/oxmq-icon.png" alt="OxMQ Logo" width="160">
</p>

If you look across the modern software engineering landscape, nearly every major ecosystem has a clear, go-to standard for distributed background job processing:
* **Python** has **Celery**
* **Node.js** has **BullMQ**
* **Go** has **Asynq**

Each of these libraries provides developers with a clean, task-centric abstraction: enqueue a discrete piece of work with an execution delay, configure automatic retries with exponential backoff, coordinate parent-child dependencies, and monitor execution progress in real-time through an intuitive web dashboard.

Yet, in the **Java ecosystem**, background task processing has historically felt fragmented. Developers building scalable microservices frequently encounter a dilemma:
* **Event Streaming Engines (like Apache Kafka):** Kafka is an exceptional distributed commit log for high-volume streaming data feeds (event sourcing, telemetry, clickstreams). But Kafka is intentionally not a background task queue: it doesn’t natively support per-job delayed execution, individual task retry backoffs, step progress reporting, or parent-child DAG completion tracking without building complex external state machines.
* **Message Brokers (like RabbitMQ):** RabbitMQ is a powerhouse for AMQP message routing, but lacks native DAG dependency resolution, sliding-window rate limit token buckets, and out-of-the-box task inspection dashboards.
* **Relational Database Schedulers (Quartz, db-scheduler):** Database-backed schedulers work nicely for simple cron jobs, but at higher scale, periodic polling (`SELECT ... FOR UPDATE`) introduces database lock contention, write amplification, and unavoidable polling latency.
* **OS Thread Pool Constraints:** Prior to Java 21, running thousands of concurrent I/O-bound workers meant dedicating platform threads that consume 1MB of stack memory each, risking carrier thread starvation whenever workers waited on external HTTP APIs, LLMs, or database transactions.

Today, we are thrilled to introduce **[OxMQ](https://github.com/gaurav10610/oxmq)**: a 100% free and open-source (Apache 2.0) distributed message queue and DAG workflow engine engineered from the ground up for **Java 21 Virtual Threads (Project Loom)** and **Redis**.

---

## 💡 What Makes OxMQ Different?

OxMQ is designed around five foundational architectural pillars:

### 1. 🧵 Native Java 21 Virtual Threads (Project Loom)
OxMQ workers execute tasks directly on lightweight Virtual Threads (`Thread.ofVirtual()`). 

Unlike traditional thread pools that pin scarce operating system threads during blocking I/O (such as calling Stripe, OpenAI, or database endpoints), Virtual Threads yield their carrier thread during blocking calls and resume automatically. With less than 2KB of initial stack memory per task, a single JVM instance can comfortably manage **thousands of concurrent background jobs** without thread pool exhaustion.

```
Traditional Worker Pool:  [OS Thread 1 (Blocked)] [OS Thread 2 (Blocked)] ... 50 Threads Max
OxMQ Loom Workers:        [V-Thread 1] [V-Thread 2] ... [V-Thread 10,000+] (Zero OS Thread Blocking)
```

### 2. ⚡ Atomic Redis Lua State Machine
All core state transitions in OxMQ (`WAITING` $\rightarrow$ `ACTIVE` $\rightarrow$ `COMPLETED` / `FAILED` / `DELAYED`) are executed within atomic Redis Lua scripts. 

Lock claiming, heartbeats, exponential backoff retries, and delay timers evaluate in sub-millisecond time directly in memory. There are **zero race conditions**, even across large clusters of competing worker nodes.

### 3. 🌲 Free & Built-In Parent-Child DAG Workflows (`FlowProducer`)
Complex background processing rarely happens in isolation. Often, you need to coordinate multi-stage pipelines:
> *"Download 10 video chunks in parallel $\rightarrow$ once all 10 chunks finish, assemble the master video stream $\rightarrow$ then send a notification."*

In OxMQ, directed acyclic graph (DAG) execution is a first-class citizen via `FlowProducer`. Parent jobs remain dormant in Redis in a `WAITING_CHILDREN` state while child tasks execute concurrently across worker nodes. The moment the final child completes, the Lua engine atomically moves the parent job to the active queue and injects all child return values directly into the parent context.

Best of all: unlike other Java enterprise workflow tools that lock DAGs behind expensive commercial subscriptions, **OxMQ’s DAG engine is 100% free under the Apache 2.0 license.**

### 4. 🌐 BullMQ Wire-Compatibility & Instant Bull-Board UI
OxMQ implements the exact Redis data structures used by the industry-standard **BullMQ v5** protocol. This unlocks two massive advantages:
1. **Polyglot Microservices:** A Node.js API can enqueue a task that a Java 21 worker consumes, or vice versa, over the same Redis cluster.
2. **Instant Web Dashboard:** You can immediately inspect queues, retry failed jobs, view real-time progress bars, and examine step logs using the open-source **[Bull-Board Web UI](https://github.com/felixmosh/bull-board)** with zero custom UI coding.

### 5. 🍃 First-Class Spring Boot 3 Integration
With `oxmq-spring-boot-starter`, registering workers is as simple as adding an annotation:

```java
@Component
public class PaymentWorker {

    @OxmqListener(queue = "payments", concurrency = 50, rateLimitMax = 100, rateLimitDurationMs = 60000)
    public PaymentResult processPayment(Job<PaymentPayload> job) {
        job.updateProgress(25);
        // Automatically runs on a lightweight Java 21 Virtual Thread!
        return paymentGateway.charge(job.getData());
    }
}
```

---

## 🎬 The Job Lifecycle in Action

Here is a visual overview of how jobs traverse OxMQ—from enqueueing, atomic state transitions, Virtual Thread dispatching, real-time progress streaming, to completion and retry backoffs:

<p align="center">
  <img src="https://raw.githubusercontent.com/gaurav10610/oxmq/main/docs/assets/oxmq-job-lifecycle.gif" alt="OxMQ Job Lifecycle" width="100%">
</p>

---

## 🚀 60-Second Quickstart

Let’s see how effortless it is to produce and consume jobs using pure Java 21.

### 1. Add the Dependency (Maven)
OxMQ is available immediately via [JitPack](https://jitpack.io/#gaurav10610/oxmq):

```xml
<repositories>
    <repository>
        <id>jitpack.io</id>
        <url>https://jitpack.io</url>
    </repository>
</repositories>

<dependencies>
    <dependency>
        <groupId>com.github.gaurav10610.oxmq</groupId>
        <artifactId>oxmq-core</artifactId>
        <version>v1.0.1</version>
    </dependency>
</dependencies>
```

### 2. Produce a Job (5 Lines of Code)
OxMQ natively serializes Java 21 `record` classes to JSON:

```java
import io.oxmq.OxmqQueue;
import io.oxmq.model.JobOptions;
import java.time.Duration;

// 1. Define payload with Java 21 Record
public record EmailNotification(String to, String subject, String body) {}

// 2. Initialize Queue
OxmqQueue<EmailNotification> queue = OxmqQueue.<EmailNotification>builder()
    .name("notifications")
    .redisUri("redis://localhost:6379")
    .build();

// 3. Enqueue with 5s delay, 3 retries, and exponential backoff
queue.add("welcome-email", 
    new EmailNotification("alice@example.com", "Welcome!", "Hello Alice!"),
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
        job.log("Dispatching email to " + job.getData().to());
        
        // Blocking I/O does NOT block OS carrier threads
        emailService.send(job.getData());
        return "DELIVERED";
    })
    .build();

worker.start();
```

---

## 🌲 Complex Workflows Made Simple: Parent-Child DAGs

Here’s how you can declare a multi-stage dependency tree using `FlowProducer`:

```java
FlowProducer flowProducer = new FlowProducer("redis://localhost:6379");

// Define parallel child tasks
FlowJobNode chunk1 = FlowJobNode.builder()
    .queueName("transcode-queue")
    .name("chunk-1")
    .data(new VideoChunk("video-101", 1))
    .build();

FlowJobNode chunk2 = FlowJobNode.builder()
    .queueName("transcode-queue")
    .name("chunk-2")
    .data(new VideoChunk("video-101", 2))
    .build();

// Define parent assembly job that triggers when children complete
FlowJobNode parentJob = FlowJobNode.builder()
    .queueName("assembly-queue")
    .name("assemble-video")
    .data(new VideoAssembly("video-101"))
    .children(List.of(chunk1, chunk2))
    .build();

flowProducer.add(parentJob);
```

The parent job automatically waits in Redis until both child tasks finish successfully. Once done, the parent wakes up, receives the return values of all child chunks, and produces the final output.

---

## 🌉 Flagship Reference Showcase: `CloudBridge`

To showcase OxMQ in a production context, the repository includes **CloudBridge** (`oxmq-examples/cloudbridge`), a complete multi-cloud asset backup pipeline:
1. It ingests repository file trees from **GitHub**.
2. Dispatches an OxMQ DAG workflow with parallel file transfer workers.
3. Concurrently uploads files to **Dropbox** (API v2) and **Box** (Content API) with chunked streaming and automatic file collision versioning.
4. Aggregates results into a parent `SyncManifest`.
5. Provides an interactive, dark-mode Web UI with live Redis DAG animation and an embedded Bull-Board dashboard:

<p align="center">
  <img src="https://raw.githubusercontent.com/gaurav10610/oxmq/main/docs/assets/oxmq-dag-workflow.gif" alt="CloudBridge DAG Workflow" width="100%">
</p>

You can launch the entire stack in under 60 seconds:
```bash
docker compose up -d
./mvnw spring-boot:run -pl oxmq-examples/cloudbridge
# Open http://localhost:8080
```

---

## 🖥️ Instant Bull-Board UI

Because OxMQ maintains 100% wire-compatibility with BullMQ, you can spin up the Bull-Board dashboard for your Java queues in one shell command:

```bash
npx @bull-board/cli --redis redis://localhost:6379 --queues notifications,transcode-queue,assembly-queue
```

Navigate to `http://localhost:3000` to inspect active workers, examine job payloads, pause/resume queues, and view live step logs in real time.

---

## 🤝 Join the Journey

OxMQ was built to give the modern Java community a first-class, lightweight, high-performance distributed task engine that feels as natural and intuitive as Celery or BullMQ, supercharged by Java 21 Virtual Threads.

The project is completely open source under the **Apache License 2.0**:
* ⭐ **GitHub Repository:** [github.com/gaurav10610/oxmq](https://github.com/gaurav10610/oxmq)
* 📖 **Documentation & Architecture:** [OxMQ Technical Guides](https://github.com/gaurav10610/oxmq/tree/main/docs)
* 📦 **Releases:** [GitHub Releases](https://github.com/gaurav10610/oxmq/releases)

If you’re building microservices with Java 21 and Spring Boot, we’d love for you to give OxMQ a spin, star the repository, and share your feedback!

---

### About the Author
**Gaurav Kumar Yadav** is a software architect and the creator of OxMQ.  
Connect on LinkedIn: [linkedin.com/in/gaurav-kumar-yadav-6125817a](https://www.linkedin.com/in/gaurav-kumar-yadav-6125817a/)  
Follow on GitHub: [@gaurav10610](https://github.com/gaurav10610)
