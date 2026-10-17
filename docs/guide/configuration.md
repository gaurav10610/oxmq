# Configuration Reference

This guide provides an enterprise-grade, comprehensive reference for all configuration properties, annotations, builder options, and runtime settings available across **OxMQ**.

---

## 🍃 Spring Boot Properties (`application.yml`)

When using `oxmq-spring-boot-starter`, configure your broker settings under the `oxmq` root prefix:

| Property | Type | Default | Description |
| :--- | :--- | :--- | :--- |
| `oxmq.namespace` | `String` | `${spring.application.name}` | Service-level context isolation namespace. Automatically prefixes all queues, workers, keys, events, and DAGs for multi-tenant Redis sharing. |
| `oxmq.prefix` | `String` | `"bull"` | Underlying Redis key prefix. If `oxmq.namespace` is specified, it takes precedence as the isolation boundary. |
| `oxmq.redis.uri` | `String` | `redis://localhost:6379` | Full Redis connection URI (supports `redis://` and `rediss://` for TLS/SSL). |
| `oxmq.redis.host` | `String` | `localhost` | Redis server hostname (used if `oxmq.redis.uri` is not specified). |
| `oxmq.redis.port` | `int` | `6379` | Redis server port. |
| `oxmq.redis.password` | `String` | `null` | Redis AUTH password (optional). |
| `oxmq.default-concurrency` | `int` | `20` | Default worker concurrency applied to `@OxmqListener` and `@OxmqBatchListener` when unspecified. |
| `oxmq.virtual-threads` | `boolean` | `true` | Whether to dispatch worker tasks to Java 21 Project Loom Virtual Threads. Transparently falls back to an optimized platform thread pool on Java 17 LTS. |
| `oxmq.metrics-enabled` | `boolean` | `true` | Exposes native Micrometer counters, timers, and gauges for Prometheus and Spring Boot Actuator. |
| `oxmq.max-len-events` | `int` | `10000` | Redis stream event trimming threshold (`MAXLEN ~ count`) on the queue's `events` key to prevent unbounded Redis stream growth. |
| `oxmq.default-job-options.remove-on-complete.count` | `Integer` | `null` | Max completed jobs retained in Redis sorted set across all queues (auto-inherited if job options are omitted). |
| `oxmq.default-job-options.remove-on-complete.age-seconds` | `Long` | `null` | Max age in seconds of completed jobs retained in Redis before automatic eviction. |
| `oxmq.default-job-options.remove-on-fail.count` | `Integer` | `null` | Max failed jobs retained in Redis sorted set across all queues. |
| `oxmq.default-job-options.remove-on-fail.age-seconds` | `Long` | `null` | Max age in seconds of failed jobs retained in Redis before automatic eviction. |

### Complete `application.yml` Example

```yaml
spring:
  application:
    name: order-service             # Automatically inherited as oxmq.namespace!

oxmq:
  namespace: order-service         # Explicit context isolation boundary
  redis:
    uri: redis://shared-redis-cluster:6379
  default-concurrency: 50          # 50 Virtual Threads per listener by default
  virtual-threads: true            # Java 21 Loom (auto platform fallback on Java 17)
  metrics-enabled: true
  max-len-events: 10000            # Bounds events stream in Redis
  default-job-options:             # Service-wide Redis memory retention policies
    remove-on-complete:
      count: 1000                  # Keep latest 1,000 completed jobs
      age-seconds: 86400           # Purge completed jobs older than 24h
    remove-on-fail:
      count: 5000                  # Retain up to 5,000 failed jobs for debugging
      age-seconds: 604800          # Purge failed jobs older than 7 days

# Spring Boot Actuator Observability
management:
  endpoints:
    web:
      exposure:
        include: health, info, metrics, prometheus
  endpoint:
    health:
      show-details: always
```

---

## 🎧 Spring Boot Listener Annotations

### 1. `@OxmqListener` (Single-Job Consumer)

Annotate any Spring bean method to consume jobs 1-by-1 concurrently:

| Attribute | Type | Default | Description |
| :--- | :--- | :--- | :--- |
| `queue` | `String` | *(Required)* | Name of the Redis queue. Supports SpEL and property placeholders (e.g. `${app.queues.orders}`). |
| `prefix` | `String` | `""` | Optional explicit namespace prefix override for cross-service queue consumption. |
| `concurrency` | `int` | `20` | Number of concurrent jobs processed simultaneously on Virtual Threads. |
| `concurrencyString` | `String` | `""` | Dynamic concurrency resolved from configuration properties (e.g. `${app.concurrency.orders:20}`). |
| `virtualThreads` | `boolean` | `true` | Whether to dispatch on Virtual Threads (Java 21+) or platform threads (Java 17). |
| `lockDurationMs` | `long` | `30000` | Distributed lock TTL in milliseconds. Auto-extended by background watchdog heartbeat. |
| `pollIntervalMs` | `long` | `50` | Sleep interval in milliseconds when queue is drained. |
| `rateLimitMax` | `int` | `0` | Max executions allowed within `rateLimitDurationMs` (`0` means unlimited). |
| `rateLimitDurationMs` | `long` | `1000` | Sliding window duration in milliseconds for the token-bucket rate limiter. |

```java
@Component
public class PaymentWorker {

    @OxmqListener(
        queue = "${app.queues.payments}",
        concurrencyString = "${app.workers.concurrency:50}",
        rateLimitMax = 100,
        rateLimitDurationMs = 1000
    )
    public PaymentResult processPayment(
            Job<PaymentPayload> job,
            @Header("jobId") String jobId,
            @Header("timestamp") long timestamp
    ) {
        job.updateProgress(25);
        return paymentService.charge(job.getData());
    }
}
```

### 2. `@OxmqBatchListener` (Bulk Dequeue Consumer)

Pulls up to `batchSize` jobs atomically in a single Redis call for batch database ingestion:

| Attribute | Type | Default | Description |
| :--- | :--- | :--- | :--- |
| `queue` | `String` | *(Required)* | Target queue name. |
| `prefix` | `String` | `""` | Optional namespace prefix override. |
| `batchSize` | `int` | `100` | Maximum number of jobs dequeued in a single atomic transaction. |
| `batchTimeoutMs` | `long` | `200` | Maximum wait duration in milliseconds before flushing an incomplete batch. |
| `concurrency` | `int` | `1` | Number of parallel batch processors. |
| `virtualThreads` | `boolean` | `true` | Execute batch handlers on Virtual Threads. |

```java
@Component
public class AuditBatchWorker {

    @OxmqBatchListener(queue = "audit-events", batchSize = 100, batchTimeoutMs = 250)
    public void ingestAuditBatch(List<Job<AuditEvent>> batch) {
        List<AuditEvent> payloads = batch.stream().map(Job::getData).toList();
        clickHouseService.batchInsert(payloads);
    }
}
```

### 3. Class-Level `@OxmqListener` + `@OxmqHandler` (Multi-Event Routing)

Routes multiple job types on a single queue to dedicated handler methods (patterned after Spring Kafka `@KafkaHandler`):

```java
@Component
@OxmqListener(queue = "notifications", concurrency = 10)
public class NotificationRouter {

    @OxmqHandler(name = "email")
    public void handleEmail(EmailNotification email) {
        emailService.send(email);
    }

    @OxmqHandler(name = "sms")
    public void handleSms(SmsNotification sms) {
        smsService.send(sms);
    }
}
```

---

## ☕ Core Engine Builders & Configuration

### 1. `QueueOptions.Builder`

Defines default settings and namespacing for an `OxmqQueue`:

```java
QueueOptions options = QueueOptions.builder()
    .prefix("billing-service")                 // Service isolation prefix (default: "bull")
    .defaultJobOptions(JobOptions.builder()     // Default options applied to all enqueued jobs
        .attempts(3)
        .exponentialBackoff(Duration.ofSeconds(1), Duration.ofSeconds(30))
        .removeOnComplete(KeepJobs.of(1000, Duration.ofHours(2)))
        .build())
    .build();
```

### 2. `OxmqQueue.Builder<T>`

```java
OxmqQueue<OrderPayload> queue = OxmqQueue.<OrderPayload>builder()
    .name("orders")                            // Required: Queue name
    .redisUri("redis://localhost:6379")        // Redis connection URI (Lettuce driver)
    .payloadClass(OrderPayload.class)          // Payload type for Jackson JSON deserialization
    .prefix("order-service")                   // Optional: Service isolation prefix
    .options(queueOptions)                     // Optional: QueueOptions bundle
    .defaultJobOptions(JobOptions.builder()     // Optional: Default retention & retry options
        .removeOnComplete(KeepJobs.count(1000))
        .removeOnFail(KeepJobs.age(Duration.ofDays(7)))
        .build())
    .maxLenEvents(10000)                       // Optional: Max Redis stream events before trimming
    .backend(customBackend)                    // Optional: Pluggable QueueBackend (default: RedisQueueBackend)
    .build();
```

### 3. `OxmqWorker.Builder<T>`

```java
OxmqWorker<OrderPayload> worker = OxmqWorker.<OrderPayload>builder()
    .queueName("orders")                       // Required: Queue name to consume from
    .redisUri("redis://localhost:6379")        // Redis connection URI
    .payloadClass(OrderPayload.class)          // Target payload class
    .prefix("order-service")                   // Optional: Service isolation prefix
    .concurrency(50)                           // Optional: Max concurrent jobs (default: 20)
    .useVirtualThreads(true)                   // Optional: Enable Loom (default: true, fallback on Java 17)
    .lockDuration(Duration.ofSeconds(30))      // Optional: Lock lease TTL (default: 30s)
    .pollInterval(Duration.ofMillis(50))       // Optional: Empty queue poll sleep (default: 50ms)
    .stalledInterval(Duration.ofSeconds(30))   // Optional: Crash detection interval (default: 30s)
    .maxStalledCount(1)                        // Optional: Max recoveries before marking FAILED (default: 1)
    .rateLimit(RateLimit.builder()             // Optional: Sliding-window rate limiter
        .max(100)
        .duration(Duration.ofSeconds(1))
        .build())
    .processor(job -> {                        // Required: Job processing lambda
        job.updateProgress(50);
        return paymentGateway.charge(job.getData());
    })
    .build();

worker.start();
```

### 4. `OxmqBatchWorker.Builder<T>`

```java
OxmqBatchWorker<AuditEvent> batchWorker = OxmqBatchWorker.<AuditEvent>builder()
    .queueName("audit-events")                 // Required: Queue name
    .redisUri("redis://localhost:6379")        // Redis connection URI
    .payloadClass(AuditEvent.class)            // Target payload class
    .prefix("order-service")                   // Optional: Service isolation prefix
    .batchSize(100)                            // Optional: Bulk pop up to 100 jobs (default: 100)
    .batchTimeout(Duration.ofMillis(200))      // Optional: Max wait before flushing (default: 200ms)
    .concurrency(2)                            // Optional: Number of batch dispatchers (default: 1)
    .useVirtualThreads(true)                   // Optional: Virtual Threads (default: true)
    .processor(batch -> {                      // Required: Batch processor lambda
        database.bulkInsert(batch);
        return "INSERTED_" + batch.size();
    })
    .build();

batchWorker.start();
```

### 5. `QueueEvents.Builder`

```java
QueueEvents events = QueueEvents.builder()
    .queueName("orders")                       // Required: Target queue
    .redisUri("redis://localhost:6379")        // Redis connection URI
    .prefix("order-service")                   // Optional: Isolation prefix
    .autorun(true)                             // Optional: Automatically start stream polling
    .build();

// Register callbacks
events.onCompleted((jobId, returnvalue) -> log.info("Job {} completed: {}", jobId, returnvalue));
events.onFailed((jobId, failedReason) -> log.error("Job {} failed: {}", jobId, failedReason));
```

### 6. `FlowProducer`

```java
// Centralized FlowProducer instance
FlowProducer flowProducer = new FlowProducer("redis://localhost:6379");

// Submit a single parent-child DAG workflow tree:
String rootJobId = flowProducer.add(parentFlowJob);

// Submit an atomic batch of multiple DAG workflow trees:
List<String> rootJobIds = flowProducer.addBulk(List.of(tree1, tree2, tree3));
```

---

## 🛠️ `JobOptions.Builder` Comprehensive Reference

`JobOptions` controls execution timing, retries, deduplication, rate limit partitioning, and Redis memory retention:

```java
JobOptions options = JobOptions.builder()
    // Execution Timing & Scheduling
    .delay(Duration.ofSeconds(15))             // Initial delay before becoming WAITING
    
    // Retries & Backoff
    .attempts(5)                               // Max execution attempts
    .exponentialBackoff(                       // Exponential backoff strategy
        Duration.ofSeconds(1),                 // Initial backoff: 1s, 2s, 4s, 8s, 16s...
        Duration.ofMinutes(5)                  // Maximum backoff interval cap
    )
    .fixedBackoff(Duration.ofSeconds(10))      // Or fixed backoff: 10s between every retry
    
    // Scheduling & Priority
    .priority(1)                               // Priority dispatch: lower integer = higher priority
    .lifo(false)                               // FIFO (false) vs LIFO stack (true)
    
    // Identity & Deduplication
    .jobId("tx_invoice_98765")                 // Deterministic custom job ID
    .deduplicationId("order_submit_user_42")   // Deduplication key
    .deduplication(DeduplicationOptions.builder()
        .id("cache_refresh_101")
        .keepLastIfActive(true)                // Buffer latest update while job is ACTIVE
        .ttlMs(60_000L)                        // Deduplication TTL window in milliseconds
        .build())
    .debounceDuration(Duration.ofSeconds(3))   // Trailing-edge quiet window
    
    // Multi-Tenant Partitioning
    .groupKey("tenant_acme_corp")              // Partition rate-limit bucket by tenant
    
    // Redis Memory Retention (KeepJobs)
    .removeOnComplete(KeepJobs.of(1000, Duration.ofHours(2))) // Retain last 1,000 completed jobs or < 2 hours
    .removeOnFail(KeepJobs.age(Duration.ofDays(7)))           // Purge failed jobs older than 7 days
    
    // DAG Workflow Policies
    .failParentOnFailure(true)                 // If child fails, immediately fail parent DAG job
    .removeDependencyOnFailure(false)          // If true, unlink failed child so parent can still run
    .build();
```

### `KeepJobs` Memory Retention Reference

| Method | Description |
| :--- | :--- |
| `KeepJobs.of(int count, Duration age)` | Retains up to `count` jobs, or any job younger than `age`. |
| `KeepJobs.of(int count, Duration age, int limit)` | Same as above, capping batch Redis trimming to `limit` jobs per pass. |
| `KeepJobs.count(int count)` | Retains strictly the most recent `count` jobs in the sorted set. |
| `KeepJobs.age(Duration age)` | Retains all jobs completed within the given `age` duration. |
| `KeepJobs.removeImmediately()` | Immediately deletes job hash and records from Redis upon completion ($O(1)$). |

---

## 🏢 Centralized Plain Java Client (`OxmqClient`)

For standalone Java applications without Spring Boot, `OxmqClient` acts as the service-level container:

```java
// Initialize once per microservice
OxmqClient client = OxmqClient.builder()
    .prefix("billing-service")
    .redisUri("redis://localhost:6379")
    .build();

// All queues inherit "billing-service"
OxmqQueue<Invoice> queue = client.queue("invoices", Invoice.class);

// All workers inherit "billing-service"
OxmqWorker<Invoice> worker = client.worker("invoices", job -> {
    return generatePdf(job.getData());
});
worker.start();

// Seamless cross-service switching:
OxmqClient shippingClient = client.forService("shipping-service");
shippingClient.queue("dispatch", DispatchRequest.class).add("pkg-1", request);
```
