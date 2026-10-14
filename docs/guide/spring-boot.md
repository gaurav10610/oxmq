# Spring Boot 3+ Starter

The `oxmq-spring-boot-starter` module provides full auto-configuration, declarative queue listeners via `@OxmqListener`, and automated Spring Boot Actuator telemetry.

---

## 📦 Dependency

::: code-group

```xml [Maven (JitPack - Zero Auth)]
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
        <version>1.0.0</version>
    </dependency>
</dependencies>
```

```kotlin [Gradle (JitPack - Zero Auth)]
repositories {
    mavenCentral()
    maven { url = uri("https://jitpack.io") }
}

dependencies {
    implementation("com.github.gaurav10610.oxmq:oxmq-spring-boot-starter:1.0.0")
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
        <version>1.0.0</version>
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
    implementation("io.oxmq:oxmq-spring-boot-starter:1.0.0")
}
```

:::

---

## ⚙️ Configuration (`application.yml`)

Configure Redis connection, worker defaults, and Actuator metrics in `application.yml`:

```yaml
oxmq:
  redis:
    uri: redis://localhost:6379     # Redis connection URI
  default-concurrency: 20           # Default worker concurrency
  virtual-threads: true             # Enable Java 21 Loom Virtual Threads (falls back to platform threads on Java 17)
  metrics-enabled: true             # Expose Micrometer meters

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

## 📨 Enqueueing with `OxmqTemplate`

The starter provides `OxmqTemplate`—a Spring-idiomatic client following the Template Design Pattern (similar to `KafkaTemplate` / `RabbitTemplate`):

```java
@Service
public class OrderService {

    @Autowired
    private OxmqTemplate oxmqTemplate;

    public void createOrder(OrderDto order) {
        // Enqueue with automatic JSON serialization
        Job<OrderDto> job = oxmqTemplate.send("orders-queue", "process-order", order);

        // Async enqueuing
        oxmqTemplate.sendAsync("orders-queue", "process-order", order)
                .thenAccept(j -> log.info("Enqueued: {}", j.getId()));

        // Queue control
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
        // Automatically runs on Virtual Threads in Java 21+
        return "SUCCESS";
    }
}
```

### 2. Batch Listener (`@OxmqBatchListener`)

Consume chunks of jobs atomically in a single Redis roundtrip:

```java
@Component
public class AuditListener {

    @OxmqBatchListener(queue = "${app.queues.audit}", batchSize = 100)
    public void handleAuditLogs(List<Job<AuditLogDto>> batch) {
        log.info("Ingesting batch of {} records", batch.size());
        // Batch DB insert or bulk indexing
    }
}
```

### 3. Class-Level Listener with Routing (`@OxmqListener` + `@OxmqHandler`)

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

## 🚨 Handling Non-Recoverable Errors

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
        "orders-queue": {
          "waiting": 12,
          "active": 3,
          "delayed": 0,
          "failed": 0
        }
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
