---
layout: home

hero:
  name: "OxMQ"
  text: "The Virtual Thread-Native Distributed Queue for Java 17+ & 21"
  tagline: "The modern Java alternative to BullMQ. Powered by Project Loom Virtual Threads, official BullMQ Lua scripts, native Context Isolation, and Request-Reply workflows."
  image:
    src: /assets/oxmq-logo.png
    alt: OxMQ Logo
  actions:
    - theme: brand
      text: Get Started
      link: /guide/what-is-oxmq
    - theme: alt
      text: Spring Boot Guide
      link: /guide/spring-boot
    - theme: alt
      text: View on GitHub
      link: https://github.com/gaurav10610/oxmq

features:
  - icon: 🚀
    title: Project Loom Virtual Threads
    details: Replace heavy OS threads with lightweight Virtual Threads on Java 21+ (graceful platform thread fallback on Java 17 LTS). Scale to thousands of concurrent blocking I/O jobs without carrier thread starvation.
  - icon: ⚡
    title: The Java Alternative to BullMQ
    details: Direct execution of 49 official BullMQ Lua scripts for atomic Redis state transitions, rate limiting, and parent-child workflows with zero custom Lua and 100% wire parity.
  - icon: 🧩
    title: 5 Native Plugins & Interceptors
    details: Non-intrusive JobInterceptor pipeline. Includes GroupRateLimiterPlugin, RateLimitSentinelPlugin (HTTP 429 cooldowns), PriorityQuotaGovernorPlugin, AdaptiveRateLimiterPlugin (AIMD dynamic capacity), and CheckpointStatePlugin.
  - icon: 🛡️
    title: Cloud Disaster Recovery & Checkpoints
    details: Distributed cursor checkpoints in Redis, automatic Lettuce reconnect on Redis crash, and Kubernetes graceful termination hooks for zero duplicate scans and zero data loss.
  - icon: 🛑
    title: In-Flight Job Cancellation
    details: Cooperative cancellation conforming to BullMQ v6 AbortController. Trip CancellationToken, interrupt executing Virtual Threads, and cleanly transition without retries.
  - icon: 🏢
    title: Native Context Isolation
    details: Run dozens of microservices on a single shared Redis cluster with zero key collisions or cross-talk using service-level namespace auto-inheritance.
  - icon: 📦
    title: High-Throughput Batch Dequeue
    details: Bulk dequeue up to N jobs atomically in a single Redis roundtrip with maxBatchBytes payload budget to eliminate heap bloat and boost throughput to 3,500+ jobs/s.
  - icon: 🌲
    title: Complex Flow Trees (DAGs)
    details: Orchestrate parent-child dependency trees with FlowProducer, batch DAG multi-root submission (addBulk), and automatic child result aggregation with Redis Cluster hash-tag safety.
  - icon: 🔄
    title: Request-Reply (waitUntilFinished)
    details: Await background job completion synchronously or reactively via Redis Streams and isFinished-3.lua without awkward loop polling.
  - icon: 🍃
    title: Spring Boot 3+ Enterprise Starter
    details: Declarative @OxmqListener and @OxmqBatchListener annotations, automatic JobInterceptor bean discovery, multi-event @OxmqHandler routing, and Actuator health checks.
  - icon: 📊
    title: Dual Grafana & Prometheus Dashboards
    details: Pre-provisioned dashboards for queue operations (throughput, depths, p99 latencies) and system metrics (isolated JVM Heap pools, CPU, HikariCP, adaptive capacity gauges).
  - icon: 🎛️
    title: Instant Bull-Board Web UI
    details: Inspect queue depths, active jobs, worker locks, and execution logs using the standard open-source Bull-Board dashboard with zero Node.js code.
  - icon: 🧹
    title: Schedulers & KeepJobs Pruning
    details: Cron recurring jobs, sliding-window rate limiters with group keys, in-flight DelayedException, and automated KeepJobs Redis memory pruning.
  - icon: 🧠
    title: Memory & Socket Efficiency
    details: Zero-allocation poller hot-path, ReentrantLock non-pinning locks, lazy DAG child evaluation, Lettuce connection multiplexing, and bounded 8KB stack trace guardrails.
  - icon: 📑
    title: Range Pagination & Queue Inspection
    details: High-performance atomic multi-state range pagination (getJobs), ergonomic state getters (getWaiting, getActive, etc.), filtered job counts, and rate limit window TTL inspection.
  - icon: 🌐
    title: OpenTelemetry W3C Distributed Tracing
    details: Native TraceContext propagation supporting W3C traceparent and tracestate headers across queues, workers, and parent-child DAG workflows with child span creation.
---

## ⚡ Quick Examples

::: code-group

```java [Standalone Producer & Worker]
// 1. Initialize Queue & Worker
OxmqQueue<OrderPayload> queue = OxmqQueue.<OrderPayload>builder()
        .name("orders")
        .redisUri("redis://localhost:6379")
        .payloadClass(OrderPayload.class)
        .build();

OxmqWorker<OrderPayload> worker = OxmqWorker.<OrderPayload>builder()
        .queueName("orders")
        .redisUri("redis://localhost:6379")
        .concurrency(50) // 50 Virtual Threads on Java 21+
        .processor(job -> {
            job.updateProgress(50);
            return paymentGateway.charge(job.getData());
        })
        .build();
worker.start();

// 2. Enqueue with retry, backoff, and memory retention
Job<OrderPayload> job = queue.add("charge-card", 
        new OrderPayload("ord_101", 99.00),
        JobOptions.builder()
                .attempts(3)
                .exponentialBackoff(Duration.ofSeconds(1), Duration.ofSeconds(30))
                .removeOnComplete(KeepJobs.of(1000, Duration.ofHours(2)))
                .build());
```

```java [In-Flight Cancellation]
// Cooperative AbortSignal parity on Java 21 Virtual Threads
OxmqWorker<VideoTask> worker = OxmqWorker.<VideoTask>builder()
        .queueName("video-rendering")
        .redisUri("redis://localhost:6379")
        .processor(job -> {
            // Listen to cooperative cancellation token
            job.getCancellationToken().onCancel(reason -> cleanupTempFiles(job.getId()));
            
            while (!job.isCancelled()) {
                renderNextFrame();
            }
            throw new JobCancelledException(job.getCancelReason());
        })
        .build();
worker.start();

// Externally cancel long-running job by ID
worker.cancelJob("video-job-42", "User clicked abort");
```

```java [High-Throughput Batch Worker]
// Dequeue up to 100 jobs atomically with maxBatchBytes memory guardrail
OxmqBatchWorker<AuditRecord> batchWorker = OxmqBatchWorker.<AuditRecord>builder()
        .queueName("audit-stream")
        .redisUri("redis://localhost:6379")
        .batchSize(100)
        .maxBatchBytes(16 * 1024 * 1024) // 16 MB payload memory safeguard
        .batchTimeout(Duration.ofMillis(200))
        .processor(batch -> {
            List<AuditRecord> records = batch.stream().map(Job::getData).toList();
            clickHouseService.batchInsert(records); // Single bulk JDBC transaction!
            return "INSERTED_" + records.size();
        })
        .build();
batchWorker.start();
```

```java [Spring Boot 3 Listener]
@Component
public class PaymentWorker {

    // Automatically runs on lightweight Virtual Threads in Java 21+
    @OxmqListener(queue = "orders", concurrency = 50, rateLimitMax = 100, rateLimitDurationMs = 1000)
    public PaymentResult handlePayment(Job<OrderPayload> job) {
        job.updateProgress(25);
        job.log("Charging credit card for order " + job.getData().orderId());
        return paymentGateway.charge(job.getData());
    }
}
```

```java [Request-Reply in REST Controller]
@RestController
@RequestMapping("/api/orders")
public class OrderController {

    @Autowired
    private OxmqTemplate oxmqTemplate;

    @PostMapping("/checkout")
    public CompletableFuture<ResponseEntity<PaymentResult>> checkout(@RequestBody OrderPayload order) {
        Job<OrderPayload> job = oxmqTemplate.send("orders", "charge-card", order);
        
        // Reactively await completion via Redis stream events without blocking carrier threads
        return oxmqTemplate.waitUntilFinishedAsync("orders", job.getId(), Duration.ofSeconds(15))
                .thenApply(res -> ResponseEntity.ok((PaymentResult) res))
                .exceptionally(ex -> ResponseEntity.status(HttpStatus.GATEWAY_TIMEOUT).build());
    }
}
```

```yaml [application.yml (Multi-Tenant Ready)]
spring:
  application:
    name: order-service     # Auto-inherited as default OxMQ Redis namespace!

oxmq:
  redis:
    uri: redis://shared-redis-cluster:6379
  default-concurrency: 50
  virtual-threads: true     # Java 21 Loom (auto platform fallback on Java 17)
  metrics-enabled: true
  max-len-events: 10000    # Bounds Redis event stream
  default-job-options:
    remove-on-complete:
      count: 1000          # Keep latest 1,000 completed jobs
      age-seconds: 86400   # Evict completed jobs older than 24h
```

```java [Enterprise Plugins & Quota Sentinel]
@Configuration
public class OxmqEnterprisePluginConfig {

    // 1. Dynamic Token Bucket per Tenant / Group Key
    @Bean
    public GroupRateLimiterPlugin groupRateLimiter(RedisConnectionManager conn, MeterRegistry meter) {
        return GroupRateLimiterPlugin.builder()
                .connectionManager(conn)
                .keyPrefix("oxmq:limiter:tenant")
                .groupKeyResolver(job -> ((TenantScopedPayload) job.getData()).tenantId())
                .dynamicLimit(tenantId -> licenseService.getBurstLimit(tenantId)) // e.g. 50-200 req/sec
                .windowDuration(Duration.ofSeconds(1))
                .retryDelayMs(150)
                .meterRegistry(meter)
                .build();
    }

    // 2. Upstream 429/503 Circuit Breaker Sentinel with Non-Destructive Delay
    @Bean
    public RateLimitSentinelPlugin rateLimitSentinel(RedisConnectionManager conn, MeterRegistry meter) {
        return RateLimitSentinelPlugin.builder()
                .connectionManager(conn)
                .keyPrefix("oxmq:sentinel:tenant")
                .groupKeyResolver(job -> ((TenantScopedPayload) job.getData()).tenantId())
                .errorMatcher(t -> t instanceof RateLimitCooldownException)
                .meterRegistry(meter)
                .build();
    }

    // 3. Priority Headroom Governor: Protect real-time jobs by yielding background sweeps
    @Bean
    public PriorityQuotaGovernorPlugin quotaGovernor(GroupRateLimiterPlugin limiter, MeterRegistry meter) {
        return PriorityQuotaGovernorPlugin.builder()
                .rateLimiter(limiter)
                .groupKeyResolver(job -> ((TenantScopedPayload) job.getData()).tenantId())
                .headroomYieldThreshold(0.25) // If < 25% token headroom remains
                .priorityThreshold(1)         // Strictly protect Priority <= 1 real-time jobs
                .yieldDelayMs(200)
                .meterRegistry(meter)
                .build();
    }
}
```

:::

---

## 💼 Slashing Operational Overhead in Enterprise Architectures

OxMQ was purpose-built to eliminate the real-world operational bottlenecks and high memory costs that plague conventional Java queue architectures:

| Conventional Java Queue Bottleneck | How OxMQ Eliminates the Overhead | Real-World Enterprise Impact |
| :--- | :--- | :--- |
| **Heavy OS Platform Threads**<br/>Each worker thread consumes ~1 MB native stack. Under high I/O concurrency (HTTP calls, DB queries), thread pools exhaust memory and collapse. | **Project Loom Virtual Threads**<br/>Each task executes on a lightweight virtual thread (~1 KB heap) that unmounts from carrier OS threads during I/O. | **10,000+ concurrent workers** on a single node without thread starvation or out-of-memory errors. |
| **Redis Network Roundtrip Storms**<br/>Dequeuing 10,000 jobs individually triggers 10,000 network roundtrips, causing CPU spikes on Redis and microsecond latency accumulation. | **Atomic Batch Dequeue (`OxmqBatchWorker`)**<br/>Bulk pops up to $N$ jobs in a single roundtrip via Lua script, paired with `maxBatchBytes` memory safety. | **10x-50x reduction in network I/O**, vaulting ingestion throughput past **3,500+ jobs/s** for ClickHouse, Elasticsearch, and JDBC batching. |
| **Redis Multi-Tenant Key Collisions**<br/>Microservices sharing a single Redis instance accidentally collide on common queue names (`orders`, `notifications`), cross-consuming jobs. | **Native Context Isolation**<br/>Automatically prefixes keys as `{service:queue}` using the "Define Once, Inherit Everywhere" pattern (`spring.application.name`). | **Zero data cross-talk** and seamless multi-tenancy on shared Redis clusters without hardcoded prefix sprawl. |
| **Upstream 429 Bans & Multi-Tenant Quota Starvation**<br/>Aggressive background workers exhaust external API quotas (Microsoft Graph, OpenAI, Stripe), triggering cluster-wide IP bans and starving real-time users. | **Native Interceptors & Plugins SPI**<br/>Dynamic tenant token buckets (`GroupRateLimiterPlugin`), distributed 429 sentinels (`RateLimitSentinelPlugin`), and priority headroom yielding (`PriorityQuotaGovernorPlugin`) non-destructively delay jobs without burning retry attempts. | **100% reliable third-party API integration** with zero 429 unhandled failures, fair quota allocation, and zero upstream Lua modifications. |
| **Redis Memory Creep & Unbounded Hash Growth**<br/>Completed and failed job records accumulate indefinitely until Redis hits `maxmemory` and begins rejecting writes. | **Automated `KeepJobs` Retention**<br/>Atomic trimming by count, age, and batch limit built directly into completion Lua scripts, plus 8KB stack trace guardrails. | **Deterministic, bounded Redis RAM footprint** with automatic queue-level inheritance and zero manual maintenance cron jobs. |
| **Redis Cluster `CROSSSLOT` Errors in DAGs**<br/>Parent-child workflow keys scattered across different hash slots crash Redis Cluster multi-key Lua commands. | **Hash-Tag DAG Routing (`FlowProducer.forCluster`)**<br/>Wraps parent and all leaf child keys in a shared `{clusterTag}` CRC16 slot. | **100% reliable multi-queue DAG execution** on distributed Redis Cluster shards with zero cross-slot failures. |


