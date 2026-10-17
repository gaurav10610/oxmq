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
    details: Replace heavy OS threads with lightweight Virtual Threads on Java 21+ (graceful platform thread fallback on Java 17 LTS). Scale to thousands of concurrent blocking I/O jobs without pool exhaustion.
  - icon: ⚡
    title: The Java Alternative to BullMQ
    details: Direct execution of 49 official BullMQ Lua scripts for atomic Redis state transitions, rate limiting, and parent-child workflows with zero custom Lua and 100% wire parity.
  - icon: 🛡️
    title: Native Context Isolation
    details: Run dozens of microservices on a single shared Redis cluster with zero key collisions or cross-talk using service-level namespace auto-inheritance.
  - icon: 🔄
    title: Request-Reply (waitUntilFinished)
    details: Await background job completion synchronously or reactively via Redis Streams and isFinished-3.lua without awkward loop polling.
  - icon: 🍃
    title: Spring Boot 3+ Enterprise Starter
    details: Declarative @OxmqListener and @OxmqBatchListener annotations, multi-event @OxmqHandler routing, and Spring Boot Actuator health checks.
  - icon: 🌲
    title: Complex Flow Trees (DAGs)
    details: Orchestrate parent-child dependency trees with FlowProducer, batch DAG multi-root submission (addBulk), and automatic child result aggregation.
  - icon: 🎛️
    title: Instant Bull-Board Web UI
    details: Inspect queue depths, active jobs, worker locks, and execution logs using the standard open-source Bull-Board dashboard with zero Node.js code.
  - icon: 🧹
    title: Schedulers & KeepJobs Pruning
    details: Cron recurring jobs, sliding-window rate limiters with group keys, in-flight DelayedException, and automated KeepJobs Redis memory pruning.
---

## Quick Example

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
```

:::

