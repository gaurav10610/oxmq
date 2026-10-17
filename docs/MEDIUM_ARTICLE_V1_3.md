# Beyond Basic Queues: How OxMQ Brought Multi-Tenant Context Isolation, Request-Reply, and Full BullMQ Parity to Java & Redis

### *From Virtual Threads to Enterprise Microservices: What’s New in OxMQ 1.3*

<p align="center">
  <img src="https://raw.githubusercontent.com/gaurav10610/oxmq/main/docs/assets/oxmq-banner.png" alt="OxMQ Banner" width="100%">
</p>

A few weeks ago, we announced the initial launch of **[OxMQ](https://github.com/gaurav10610/oxmq)**—the first distributed task queue engine built specifically for **Java 21 Project Loom Virtual Threads**, paired with the wire-compatible Redis Lua script engine of **BullMQ**.

The response from the developer community was overwhelmingly positive. Java engineers finally had a high-throughput, virtual-thread-native alternative to heavy legacy task brokers, complete with zero-Node.js interop with the [Bull-Board Web UI](https://github.com/felixmosh/bull-board) and parent-child DAG orchestration.

But as teams began spinning up OxMQ in complex distributed environments, real-world enterprise architectures posed harder questions:

> *"In our Kubernetes cluster, dozens of microservices share a single central Redis cluster. How do we keep Service A’s queues completely isolated from Service B’s without manually prefixing every queue string across our codebase?"*  
> *"Sometimes our REST API triggers an asynchronous job, but the HTTP client needs to wait for the computation result before returning a response. Can we do request-reply without polling in a loop?"*  
> *"If our workers process millions of jobs per hour, how do we prevent Redis memory exhaustion without writing manual cleanup cron jobs?"*  
> *"Our enterprise standard is still Java 17 LTS. Can we use OxMQ today while we prepare our Java 21 migration?"*

Today, with the release of **OxMQ v1.3.0**, we’ve answered every single one of those questions.

Here is a deep dive into the major features that transform OxMQ from a high-speed queue into an enterprise-grade distributed workflow engine.

---

## 1. 🛡️ Native Multi-Service Context Isolation & Hierarchical Namespacing

In production microservice ecosystems, provisioning a dedicated Redis instance for every single microservice is an operational and financial anti-pattern. Most organizations operate a managed, multi-tenant Redis cluster (e.g., AWS ElastiCache, Azure Cache for Redis, GCP Memorystore) shared across engineering teams.

Without native context isolation, two services running queues with common names like `orders`, `notifications`, or `billing` will inadvertently steal each other's jobs, corrupt Redis stream consumers, and pollute each other’s metrics.

### The Developer Friction We Eliminated
Previously, developers had to manually prepend service tags onto every queue name: `orders-service:checkout-queue`, `orders-service:refund-queue`, etc. This leads to brittle string concatenation bugs, breaks Bull-Board dashboard setups, and creates maintenance headaches.

### The OxMQ Solution: Zero-Config Inheritance
In OxMQ v1.3, isolation is a first-class, service-level architectural construct.

#### Level A: Global Service Namespace (Spring Boot `application.yml`)
Set your service's boundary once in configuration:

```yaml
oxmq:
  redis:
    uri: redis://shared-redis-cluster:6379
  namespace: order-service     # 👈 Service-level context isolation boundary
  worker:
    concurrency: 50
```

With this single property:
- Every queue (`OxmqQueue`, `OxmqTemplate`, `@OxmqListener`) is automatically isolated under `order-service:bull:<queue-name>:*`.
- Redis Pub/Sub and Stream events are isolated to `order-service:bull:<queue-name>:events`.
- Workers running in `billing-service` (with `oxmq.namespace: billing-service`) sharing the identical Redis cluster will never touch, see, or dequeue jobs belonging to `order-service`.

#### Level B: Programmatic Thread-Local & Dynamic Scoping
For SaaS platforms where tenants share workers or jobs originate from dynamic customer contexts, OxMQ provides `OxmqContextHolder`:

```java
// Run a block of execution strictly scoped to tenant-101
OxmqContextHolder.runWithNamespace("tenant-101", () -> {
    // Automatically enqueued under tenant-101:bull:invoices:*
    oxmqTemplate.add("invoices", new InvoicePayload(orderId));
});
```

#### Level C: Unified Multi-Tenant Bull-Board UI
Because the Redis key structure adheres strictly to standard BullMQ conventions (`<prefix>:<queue-name>:<hash>`), you can mount multiple isolated service namespaces under a single Node.js Bull-Board instance with simple path-based routing:

```typescript
import { createBullBoard } from '@bull-board/api';
import { BullMQAdapter } from '@bull-board/api/bullMQAdapter';
import { Queue } from 'bullmq';

// Mount Order Service Queues
const orderQueue = new Queue('checkout', { 
  prefix: 'order-service', 
  connection: redisConfig 
});

// Mount Billing Service Queues on the same Redis cluster
const billingQueue = new Queue('invoices', { 
  prefix: 'billing-service', 
  connection: redisConfig 
});

createBullBoard({
  queues: [new BullMQAdapter(orderQueue), new BullMQAdapter(billingQueue)],
  serverAdapter: serverAdapter
});
```

---

## 2. ⚡ The Request-Reply Pattern: `waitUntilFinished`

While asynchronous background processing is ideal for decoupled workloads, modern APIs often require a synchronous or reactive **Request-Reply** interaction model:

1. User submits an AI prompt or document generation request over HTTP.
2. The REST controller enqueues a background job across a GPU/worker pool.
3. The controller awaits the computed result or a failure timeout before replying to the user.

Prior to OxMQ 1.3, developers had to implement custom polling loops with `Thread.sleep()` or wire custom Redis Pub/Sub channels.

### Enter `waitUntilFinished` and `waitUntilFinishedAsync`
OxMQ now provides native, event-driven request-reply waiting powered directly by official BullMQ Redis Stream events and the battle-tested `isFinished-3.lua` script.

### Asynchronous / Non-Blocking (CompletableFuture)
```java
@PostMapping("/generate-report")
public CompletableFuture<ResponseEntity<ReportResult>> generateReport(@RequestBody ReportRequest request) {
    // Enqueue the heavy job
    Job<ReportRequest> job = reportQueue.add("pdf-render", request);

    // Reactively await completion via Redis stream events without blocking carrier threads
    return job.waitUntilFinishedAsync(queueEvents, Duration.ofSeconds(30))
        .thenApply(result -> ResponseEntity.ok((ReportResult) result))
        .exceptionally(ex -> ResponseEntity.status(504).build());
}
```

### Synchronous Virtual-Thread Friendly
If you are running on Java 21 Virtual Threads, blocking waits have near-zero memory footprint:

```java
// Submits and blocks the virtual thread until finished or 10 seconds elapse
Job<DataPayload> job = queue.add("compute-task", new DataPayload(42));

try {
    Object result = job.waitUntilFinished(queueEvents, Duration.ofSeconds(10));
    log.info("Computation succeeded with result: {}", result);
} catch (TimeoutException e) {
    log.warn("Job did not finish in time");
} catch (ExecutionException e) {
    log.error("Job failed on worker: {}", e.getCause().getMessage());
}
```

---

## 3. 🧹 Automatic Redis Memory Pruning: `KeepJobs`

A common pitfall with high-throughput task queues is Redis memory creep. When processing 500,000 jobs per day, storing completed job hashes, return values, logs, and metadata indefinitely will quickly trigger Redis Out-Of-Memory (`OOM`) exceptions.

In OxMQ 1.3, we brought official BullMQ retention policy parity via `KeepJobs`:

```java
// Retain only the last 1,000 completed jobs, or any job completed within the last 2 hours
JobOptions options = JobOptions.builder()
    .removeOnComplete(KeepJobs.of(1000, Duration.ofHours(2)))
    // Automatically purge failed jobs older than 7 days
    .removeOnFail(KeepJobs.age(Duration.ofDays(7)))
    .build();

queue.add("transcode-task", payload, options);
```

### How It Works Under the Hood
Rather than relying on separate background cleanup timers, OxMQ wires `KeepJobs` directly into BullMQ's atomic `moveToFinished-14.lua` script. During the exact Redis transaction where a job transitions from `ACTIVE` $\rightarrow$ `COMPLETED`, the script inspects the sorted set and trims obsolete job records in a single atomic pass ($O(\log N)$).

---

## 4. 🔄 Dynamic Delays & Custom Retry Backoffs

In real-world networks, external APIs go down, payment gateways return HTTP 429 Too Many Requests, and databases hit temporary connection limits. Standard retry strategies (fixed intervals or naive exponential backoff) often fail to capture domain-specific needs.

### In-Flight Dynamic Delays (`DelayedException`)
What if an external rate-limit header returns `Retry-After: 45`? You don't want to increment the job's failure counter or trigger alerts—you simply want to pause this specific job for 45 seconds.

In OxMQ:
```java
@OxmqListener(queue = "stripe-webhooks")
public void handleWebhook(Job<WebhookPayload> job) {
    HttpResponse response = callThirdPartyApi(job.getData());
    
    if (response.statusCode() == 429) {
        long retryAfterSeconds = response.getHeader("Retry-After");
        // Atomically re-schedules job into the DELAYED set without counting as a failed attempt!
        throw new DelayedException(Duration.ofSeconds(retryAfterSeconds));
    }
}
```

### Custom Formula-Driven Backoffs
You can now define dynamic backoff algorithms tailored to your service:

```java
JobOptions options = JobOptions.builder()
    .attempts(5)
    .backoff(BackoffOptions.builder()
        .type("decorrelated-jitter")
        .delay(Duration.ofSeconds(2))
        .build())
    .build();
```

---

## 5. ☕ Dual Engine: Java 17 LTS Baseline & Java 21 Loom

While Java 21 with Project Loom represents the modern frontier of Java concurrency, enterprise surveys confirm that over **60% of enterprise production systems remain on Java 17 LTS**.

Starting in OxMQ v1.1 and hardened in v1.3:
* **Java 21+**: OxMQ automatically detects Loom Virtual Threads and provisions an unmetered, ultra-lightweight virtual thread per concurrent job.
* **Java 17 LTS**: OxMQ transparently switches to an optimized platform thread executor pool without requiring any code changes or dependency swaps.

Your development teams can deploy OxMQ on existing Java 17 microservices today, and immediately reap the scalability benefits of Virtual Threads when migrating to Java 21 tomorrow—with zero lines of queue code changed.

---

## 6. 🌲 Batch Multi-Root Flow DAG Workflows (`FlowProducer.addBulk`)

For complex orchestration (such as batch ETL pipelines, distributed video rendering, or order fulfillment sagas), `FlowProducer` allows building tree-structured Directed Acyclic Graphs (DAGs).

In v1.3, we added `FlowProducer.addBulk(List<FlowJob<?>> roots)` to atomically submit entire batches of multi-stage workflow trees in a single round-trip:

```java
FlowProducer flowProducer = new FlowProducer("redis://localhost:6379");

List<FlowJob<Void>> bulkWorkflows = new ArrayList<>();

for (BatchBatch batch : pendingBatches) {
    // Child step A: Download
    FlowJob<Void> download = FlowJob.<Void>builder()
        .queueName("download-queue")
        .name("download-" + batch.id())
        .data(batch.getDownloadSpec())
        .build();

    // Child step B: Transform
    FlowJob<Void> transform = FlowJob.<Void>builder()
        .queueName("transform-queue")
        .name("transform-" + batch.id())
        .data(batch.getTransformSpec())
        .build();

    // Parent step: Aggregate & Finalize
    FlowJob<Void> root = FlowJob.<Void>builder()
        .queueName("aggregation-queue")
        .name("aggregate-" + batch.id())
        .children(List.of(download, transform))
        .build();

    bulkWorkflows.add(root);
}

// Submits all DAG trees atomically to Redis
flowProducer.addBulk(bulkWorkflows);
```

---

## 📊 Feature Matrix: Where OxMQ Stands Today

| Feature Capability | BullMQ (Node.js) | Celery (Python) | Legacy Java Queues | OxMQ (Java 17 / 21) |
| :--- | :---: | :---: | :---: | :---: |
| **Virtual Thread Native (Loom)** | ❌ (Single-thread JS) | ❌ (Gevent/Pthreads) | ❌ (Heavy OS threads) | 🟢 **Native Java 21 Loom** |
| **Java 17 LTS Fallback** | N/A | N/A | 🟡 Partial | 🟢 **100% Native** |
| **Official BullMQ Lua Parity** | 🟢 Native | ❌ Incompatible | ❌ Incompatible | 🟢 **49/49 Verified Scripts** |
| **Multi-Service Context Isolation** | 🟡 Manual Prefixing | 🟡 Manual vhost | ❌ | 🟢 **Zero-Config Service Namespaces** |
| **Request-Reply (`waitUntilFinished`)** | 🟢 Yes | 🟡 Backend Polling | ❌ | 🟢 **Native via Redis Streams** |
| **DAG Parent-Child Workflows** | 🟢 Yes | 🟡 Canvas primitives | ❌ Paid enterprise | 🟢 **100% Free (Apache 2.0)** |
| **Dynamic In-Flight Delay** | 🟢 Yes | ❌ Retry failure | ❌ | 🟢 **`DelayedException`** |
| **Web UI Dashboard** | 🟢 Bull-Board | 🟡 Flower | ❌ Proprietary | 🟢 **Zero-Node Bull-Board Compatible** |
| **License** | MIT | BSD-3 | Commercial paywalls | 🟢 **Apache 2.0** |

---

## 🚀 Getting Started with OxMQ 1.3

Adding OxMQ 1.3 to your Maven or Gradle project takes less than 60 seconds with **JitPack** (zero authentication needed) or **GitHub Packages**:

### Maven (`pom.xml`)
```xml
<repositories>
    <repository>
        <id>jitpack.io</id>
        <url>https://jitpack.io</url>
    </repository>
</repositories>

<dependencies>
    <!-- Core Pure Java Engine -->
    <dependency>
        <groupId>com.github.gaurav10610.oxmq</groupId>
        <artifactId>oxmq-core</artifactId>
        <version>1.3.0</version>
    </dependency>

    <!-- Optional: Spring Boot 3 Starter -->
    <dependency>
        <groupId>com.github.gaurav10610.oxmq</groupId>
        <artifactId>oxmq-spring-boot-starter</artifactId>
        <version>1.3.0</version>
    </dependency>
</dependencies>
```

### Gradle (`build.gradle.kts`)
```kotlin
repositories {
    mavenCentral()
    maven { url = uri("https://jitpack.io") }
}

dependencies {
    implementation("com.github.gaurav10610.oxmq:oxmq-core:1.3.0")
    // or for Spring Boot 3:
    // implementation("com.github.gaurav10610.oxmq:oxmq-spring-boot-starter:1.3.0")
}
```

---

## 🤝 Open Source & Community

OxMQ is 100% open source under the **Apache License 2.0**. No paywalled enterprise tiers, no licensing traps, and no restrictions on clustering or parent-child DAG orchestration.

* 🌟 **GitHub Repository:** [github.com/gaurav10610/oxmq](https://github.com/gaurav10610/oxmq)
* 📚 **Interactive Documentation:** [gaurav10610.github.io/oxmq](https://gaurav10610.github.io/oxmq/)
* 📦 **Release Notes & Downloads:** [GitHub Releases v1.3.0](https://github.com/gaurav10610/oxmq/releases/tag/v1.3.0)

If your team is building distributed systems, high-throughput microservices, or DAG workflows on the JVM, we'd love for you to give OxMQ a try, star the repository, and share your feedback!

---

### About the Author
**Gaurav Kumar Yadav** is a software architect and the creator of OxMQ.  
Connect on LinkedIn: [linkedin.com/in/gaurav-kumar-yadav-6125817a](https://www.linkedin.com/in/gaurav-kumar-yadav-6125817a/)  
Follow on GitHub: [@gaurav10610](https://github.com/gaurav10610)
