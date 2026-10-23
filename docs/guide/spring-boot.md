# Spring Boot 3+ Starter

The `oxmq-spring-boot-starter` module provides full auto-configuration, native context isolation / namespacing across shared Redis clusters, declarative queue listeners via `@OxmqListener`, and automated Spring Boot Actuator telemetry.

---

## 📦 Dependency

::: code-group

```xml [Maven Central (Recommended)]
<dependencies>
    <dependency>
        <groupId>io.github.gaurav10610</groupId>
        <artifactId>oxmq-spring-boot-starter</artifactId>
        <version>1.7.0</version>
    </dependency>
</dependencies>
```

```kotlin [Gradle (Maven Central)]
dependencies {
    implementation("io.github.gaurav10610:oxmq-spring-boot-starter:1.7.0")
}
```

```xml [Maven (JitPack)]
<repositories>
    <repository>
        <id>jitpack.io</id>
        <url>https://jitpack.io</url>
    </repository>
</repositories>

<dependencies>
    <dependency>
        <groupId>com.github.gaurav10610.oxmq</groupId>
        <artifactId>oxmq-spring-boot-starter</artifactId>
        <version>1.7.0</version>
    </dependency>
</dependencies>
```

```kotlin [Gradle (JitPack)]
repositories {
    mavenCentral()
    maven { url = uri("https://jitpack.io") }
}

dependencies {
    implementation("com.github.gaurav10610.oxmq:oxmq-spring-boot-starter:1.7.0")
}
```

```xml [Maven (GitHub Packages)]
<!-- In your pom.xml -->
<repositories>
    <repository>
        <id>github</id>
        <url>https://maven.pkg.github.com/gaurav10610/oxmq</url>
    </repository>
</repositories>

<dependencies>
    <dependency>
        <groupId>io.oxmq</groupId>
        <artifactId>oxmq-spring-boot-starter</artifactId>
        <version>1.5.0</version>
    </dependency>
</dependencies>

<!-- Requires GitHub PAT with read:packages in ~/.m2/settings.xml -->
```

```kotlin [Gradle (GitHub Packages)]
repositories {
    mavenCentral()
    maven {
        url = uri("https://maven.pkg.github.com/gaurav10610/oxmq")
        credentials {
            username = project.findProperty("gpr.user") as String? ?: System.getenv("GITHUB_ACTOR")
            password = project.findProperty("gpr.key") as String? ?: System.getenv("GITHUB_TOKEN")
        }
    }
}

dependencies {
    implementation("io.oxmq:oxmq-spring-boot-starter:1.5.0")
}
```

:::

---

## ⚙️ Configuration (`application.yml`)

Configure Redis connection, worker defaults, context isolation prefix, and Actuator metrics in `application.yml`:

```yaml
spring:
  application:
    name: order-service             # Auto-inherited as default OxMQ Redis namespace prefix!

oxmq:
  prefix: order-service             # Optional explicit override (defaults to spring.application.name)
  redis:
    uri: redis://localhost:6379     # Redis connection URI
  default-concurrency: 20           # Default worker concurrency
  virtual-threads: true             # Enable Java 21 Loom Virtual Threads (falls back to platform threads on Java 17)
  metrics-enabled: true             # Expose Micrometer meters
  max-len-events: 10000            # Bounds events stream key via approximate trimming
  default-job-options:             # Service-wide Redis memory retention policies
    remove-on-complete:
      count: 1000                  # Keep latest 1,000 completed jobs
      age-seconds: 86400           # Evict completed jobs older than 24h
    remove-on-fail:
      count: 5000                  # Keep last 5,000 failed jobs for debugging
      age-seconds: 604800          # Evict failed jobs older than 7 days

app:
  queues:
    orders: orders-queue
    audit: audit-queue
    notifications: notifications-queue
  concurrency:
    orders: 10
```

---

## 🚀 Activation (`@EnableOxmq`)

Activate OxMQ in your Spring Boot application by adding `@EnableOxmq`:

```java
@SpringBootApplication
@EnableOxmq
public class Application {
    public static void main(String[] args) {
        SpringApplication.run(Application.class, args);
    }
}
```

---

## 🏢 Context Isolation & Cross-Service Messaging

OxMQ implements the **"Define Once at Service Level, Inherit Everywhere"** pattern:
- Setting `spring.application.name: order-service` automatically prefixes all queues and workers as `{order-service:queueName}:*`.
- When multiple services share a single Redis cluster, their data structures are strictly isolated with zero cross-talk.

### Cross-Service Queue Operations (`toService`)

To enqueue work into another microservice's queue on the same Redis cluster without running that service's workers:

```java
@Service
public class OrderService {

    @Autowired
    private OxmqTemplate oxmqTemplate;

    public void completeOrder(OrderDto order) {
        // Enqueues in order-service namespace (order-service:orders-queue:*)
        oxmqTemplate.send("orders-queue", "process-order", order);

        // Cross-service dispatch: Enqueue into shipping-service namespace!
        oxmqTemplate.toService("shipping-service")
                .send("dispatch-queue", new ShippingRequest(order.id(), order.address()));
    }
}
```

*For a full architectural deep dive on hash-tagging and precedence ladders, see our [Context Isolation Guide](/guide/namespacing).*

---

## 📨 Enqueueing with `OxmqTemplate`

The starter provides `OxmqTemplate`—a Spring-idiomatic client following the Template Design Pattern:

```java
@Service
public class OrderService {

    @Autowired
    private OxmqTemplate oxmqTemplate;

    public void createOrder(OrderDto order) {
        // Enqueue with automatic JSON serialization and KeepJobs retention
        Job<OrderDto> job = oxmqTemplate.send("orders-queue", "process-order", order,
                JobOptions.builder()
                        .attempts(3)
                        .removeOnComplete(KeepJobs.of(1000, Duration.ofHours(2)))
                        .build());

        // Async enqueuing
        oxmqTemplate.sendAsync("orders-queue", "process-order", order)
                .thenAccept(j -> log.info("Enqueued: {}", j.getId()));

        // Synchronous Request-Reply: Await worker result with timeout
        Object syncResult = oxmqTemplate.waitUntilFinished("orders-queue", job.getId(), Duration.ofSeconds(10));

        // Asynchronous Request-Reply: CompletableFuture non-blocking waiting
        CompletableFuture<Object> asyncResult = oxmqTemplate.waitUntilFinishedAsync("orders-queue", job.getId(), Duration.ofSeconds(30));

        // Queue lifecycle management
        oxmqTemplate.pause("orders-queue");
        oxmqTemplate.resume("orders-queue");
    }
}
```

---

## 🎧 Declarative Listeners

### 1. Method-Level Listener (`@OxmqListener`)

Annotate any bean method with `@OxmqListener`. Supports property placeholders (`${app.queues.orders}`) and `@Header` metadata injection:

```java
@Component
public class OrderListener {

    @OxmqListener(
        queue = "${app.queues.orders:orders-queue}",
        concurrencyString = "${app.concurrency.orders:10}"
    )
    public String handleOrder(
            OrderDto order,
            @Header("jobId") String jobId,
            @Header("timestamp") long timestamp
    ) {
        log.info("Processing order {} (jobId={})", order.orderId(), jobId);
        // Automatically runs on Virtual Threads in Java 21+ (or platform threads in Java 17)
        return "SUCCESS";
    }
}
```

### 2. Explicit Namespace Override on Listener

To listen directly to a queue in another service's namespace:

```java
@Component
public class CrossServiceAuditListener {

    @OxmqListener(queue = "audit-events", prefix = "security-service")
    public void onSecurityEvent(SecurityAuditDto audit) {
        log.info("Received external security event: {}", audit.action());
    }
}
```

### 3. Batch Listener (`@OxmqBatchListener`)

Consume chunks of jobs atomically in a single Redis roundtrip:

```java
@Component
public class AuditListener {

    // Consume up to 100 jobs atomically with maxBatchBytes heap safeguard
    @OxmqBatchListener(queue = "${app.queues.audit}", batchSize = 100, maxBatchBytes = 16 * 1024 * 1024)
    public void handleAuditLogs(List<Job<AuditLogDto>> batch) {
        log.info("Ingesting batch of {} records", batch.size());
        // Batch DB insert or bulk indexing
    }
}
```

### 4. Class-Level Listener with Routing (`@OxmqListener` + `@OxmqHandler`)

Patterned after Spring Kafka `@KafkaHandler`, routes multiple job types on a single queue:

```java
@Component
@OxmqListener(queue = "${app.queues.notifications}", concurrency = 5)
public class NotificationListener {

    @OxmqHandler(name = "email")
    public void handleEmail(NotificationDto email) {
        emailService.send(email);
    }

    @OxmqHandler(name = "sms")
    public void handleSms(NotificationDto sms) {
        smsService.send(sms);
    }
}
```

---

## 🚨 Handling Non-Recoverable Errors & Postponement

### Immediate Failure Without Retries (`UnrecoverableError`)

If an error is unrecoverable (e.g. invalid user input, permanent authentication failure), throw `UnrecoverableError`. OxMQ will fail the job immediately without retrying:

```java
import io.oxmq.exception.UnrecoverableError;

@OxmqListener(queue = "payments")
public void processPayment(Job<PaymentRequest> job) {
    PaymentRequest payment = job.getData();

    if (payment.amount() <= 0) {
        throw new UnrecoverableError("Invalid payment amount: " + payment.amount());
    }

    paymentService.charge(payment);
}
```

### Worker Postponement (`DelayedException`)

If an external service is temporarily down or rate-limited, throw `DelayedException` to postpone execution for a specific duration without consuming an attempt or recording a failure:

```java
import io.oxmq.exception.DelayedException;

@OxmqListener(queue = "webhooks")
public void deliverWebhook(Job<WebhookPayload> job) {
    if (downstreamService.isMaintenanceWindow()) {
        // Postpone job for 60 seconds without incrementing attemptsMade
        throw new DelayedException(60_000L);
    }
    downstreamService.post(job.getData());
}
```

---

## 📊 Spring Boot Actuator Integration

OxMQ exposes real-time queue health and metrics through Spring Boot Actuator.

### Health Indicator (`/actuator/health`)

```json
{
  "status": "UP",
  "components": {
    "oxmq": {
      "status": "UP",
      "details": {
        "redis": "CONNECTED",
        "namespace": "order-service",
        "registeredWorkers": 3,
        "workersRunning": true
      }
    }
  }
}
```

### Micrometer Metrics (`/actuator/metrics`)

- `oxmq.jobs.processed`: Counter of successfully completed jobs.
- `oxmq.jobs.failed`: Counter of failed jobs.
- `oxmq.jobs.duration`: Timer recording execution duration per queue.
- `oxmq.queue.size`: Gauge measuring waiting jobs.
- `oxmq.plugin.ratelimiter.yields`: Counter of jobs deferred by token bucket exhaustion.
- `oxmq.plugin.sentinel.delays`: Counter of jobs deferred by upstream HTTP 429 sentinel locks.
- `oxmq.plugin.governor.yields`: Counter of low-priority jobs yielded to protect high-priority headroom.

---

## 🧩 Auto-Configured Plugins & Interceptors SPI

Any Spring bean implementing `JobInterceptor` is automatically detected by `OxmqListenerAnnotationBeanPostProcessor` and wired into all `@OxmqListener` and `@OxmqBatchListener` instances:

```java
@Configuration
public class EnterprisePluginConfiguration {

    // 1. Dynamic Token Bucket per Tenant / Group
    @Bean
    public GroupRateLimiterPlugin groupRateLimiter(
            RedisConnectionManager connectionManager,
            MeterRegistry meterRegistry
    ) {
        return GroupRateLimiterPlugin.builder()
                .connectionManager(connectionManager)
                .keyPrefix("oxmq:limiter:tenant")
                .groupKeyResolver(job -> ((TenantScopedPayload) job.getData()).tenantId())
                .dynamicLimit(tenantId -> 100) // 100 req/sec per tenant
                .windowDuration(Duration.ofSeconds(1))
                .retryDelayMs(150)
                .meterRegistry(meterRegistry)
                .build();
    }

    // 2. Upstream 429/503 Circuit Breaker Sentinel
    @Bean
    public RateLimitSentinelPlugin rateLimitSentinel(
            RedisConnectionManager connectionManager,
            MeterRegistry meterRegistry
    ) {
        return RateLimitSentinelPlugin.builder()
                .connectionManager(connectionManager)
                .keyPrefix("oxmq:sentinel:tenant")
                .groupKeyResolver(job -> ((TenantScopedPayload) job.getData()).tenantId())
                .errorMatcher(t -> t instanceof RateLimitCooldownException)
                .meterRegistry(meterRegistry)
                .build();
    }

    // 3. Priority Headroom Governor
    @Bean
    public PriorityQuotaGovernorPlugin quotaGovernor(
            GroupRateLimiterPlugin rateLimiter,
            MeterRegistry meterRegistry
    ) {
        return PriorityQuotaGovernorPlugin.builder()
                .rateLimiter(rateLimiter)
                .groupKeyResolver(job -> ((TenantScopedPayload) job.getData()).tenantId())
                .headroomYieldThreshold(0.25) // Reserve 25% quota for priority jobs
                .priorityThreshold(1)         // Protect priority <= 1
                .yieldDelayMs(200)
                .meterRegistry(meterRegistry)
                .build();
    }
}
```

*For more details on custom interceptors, see the [Plugins & Interceptors Guide](/guide/plugins).*

---

## 📈 Bundled Grafana Dashboard Template

The starter packages a complete, enterprise-grade Grafana dashboard at:
`oxmq-spring-boot-starter/src/main/resources/dashboards/oxmq-grafana-dashboard.json`

Import this dashboard into Grafana connected to your Prometheus instance (`/actuator/prometheus`) for live queue gauges, throughput charts, error percentiles, and multi-tenant quota yields.

