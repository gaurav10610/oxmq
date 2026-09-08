# 🚀 Getting Started with OxMQ
### *The High-Performance, Virtual Thread-Native Distributed Job Queue for Java 21+*

Welcome to **OxMQ**, the production-grade distributed message queue and DAG workflow engine engineered natively for **Java 21 (Project Loom Virtual Threads)** and **Redis**.

This guide walks you from zero to production background job processing in minutes.

---

## 📦 1. Installation

OxMQ is available via **JitPack** or as direct JAR binaries from our [GitHub Releases](https://github.com/gaurav10610/oxmq/releases/tag/v1.0.0).

### Maven (`pom.xml`)

Add the JitPack repository and the OxMQ dependency:

```xml
<repositories>
    <repository>
        <id>jitpack.io</id>
        <url>https://jitpack.io</url>
    </repository>
</repositories>

<dependencies>
    <!-- Core Pure Java Engine (Virtual Threads + Redis) -->
    <dependency>
        <groupId>com.github.gaurav10610.oxmq</groupId>
        <artifactId>oxmq-core</artifactId>
        <version>v1.0.0</version>
    </dependency>

    <!-- Optional: Spring Boot 3 Starter (@OxmqListener, Actuator) -->
    <dependency>
        <groupId>com.github.gaurav10610.oxmq</groupId>
        <artifactId>oxmq-spring-boot-starter</artifactId>
        <version>v1.0.0</version>
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
    implementation("com.github.gaurav10610.oxmq:oxmq-core:v1.0.0")
    // or for Spring Boot 3 applications:
    // implementation("com.github.gaurav10610.oxmq:oxmq-spring-boot-starter:v1.0.0")
}
```

---

## 🗄️ 2. Starting Redis

OxMQ uses Redis 6.2+ or Redis 7.x (or Valkey) as its high-speed atomic state store.

Start a local Redis container in 1 second:
```bash
docker run -d --name oxmq-redis -p 6379:6379 redis:7-alpine
```

Or spin up our full developer observability stack (Redis + Bull-Board + Prometheus + Grafana):
```bash
docker compose up -d
```

---

## 📤 3. Producing Jobs (5 Lines of Code)

OxMQ natively serializes Java 21 `record` and POJO classes to JSON.

```java
import io.oxmq.OxmqQueue;
import io.oxmq.model.Job;
import io.oxmq.model.JobOptions;
import java.time.Duration;

// 1. Define your payload (Java 21 Records natively supported)
public record OrderInvoice(String orderId, String customerEmail, double amount) {}

public class ProducerExample {
    public static void main(String[] args) {
        // 2. Initialize Queue
        OxmqQueue<OrderInvoice> queue = OxmqQueue.<OrderInvoice>builder()
                .name("order-invoices")
                .redisUri("redis://localhost:6379")
                .payloadClass(OrderInvoice.class)
                .build();

        // 3. Enqueue with 5-second delay, 3 retries, and exponential backoff
        Job<OrderInvoice> job = queue.add(
                "generate-pdf",
                new OrderInvoice("ord_9876", "customer@example.com", 249.99),
                JobOptions.builder()
                        .delay(Duration.ofSeconds(5))
                        .attempts(3)
                        .exponentialBackoff(Duration.ofSeconds(1))
                        .build()
        );

        System.out.println("Enqueued job ID: " + job.getId());
    }
}
```

---

## ⚡ 4. Consuming Jobs with Java 21 Virtual Threads

Because OxMQ runs natively on **Java 21 Project Loom (Virtual Threads)**, you can comfortably configure concurrency of 100, 500, or 1,000+ workers per node without thread pool starvation.

```java
import io.oxmq.OxmqWorker;
import java.util.Map;

public class ConsumerExample {
    public static void main(String[] args) {
        // Initialize Worker with 100 Virtual Threads
        OxmqWorker<OrderInvoice> worker = OxmqWorker.<OrderInvoice>builder()
                .queueName("order-invoices")
                .redisUri("redis://localhost:6379")
                .payloadClass(OrderInvoice.class)
                .concurrency(100) // 100 concurrent Virtual Threads!
                .processor(job -> {
                    OrderInvoice invoice = job.getData();
                    System.out.printf("Generating invoice for order %s (VirtualThread: %b)%n",
                            invoice.orderId(), Thread.currentThread().isVirtual());

                    // Report progress in real-time
                    job.updateProgress(25);
                    job.log("Fetching order items from database...");

                    // Blocking HTTP / I/O calls do NOT block OS carrier threads!
                    job.updateProgress(75);
                    job.log("Rendering PDF invoice...");

                    job.updateProgress(100);
                    return Map.of("pdfUrl", "https://s3.amazonaws.com/invoices/" + invoice.orderId() + ".pdf");
                })
                .build();

        worker.start();
    }
}
```

---

## 🍃 5. Spring Boot 3 Quickstart

With `oxmq-spring-boot-starter`, you get declarative listeners and Actuator integration out-of-the-box.

### `application.yml`
```yaml
oxmq:
  redis:
    uri: redis://localhost:6379
  default-concurrency: 50
  virtual-threads: true
  metrics-enabled: true
```

### Application Code
```java
@SpringBootApplication
@EnableOxmq
public class BillingMicroservice {

    public static void main(String[] args) {
        SpringApplication.run(BillingMicroservice.class, args);
    }

    @Component
    public static class InvoiceListener {

        @OxmqListener(queue = "order-invoices", concurrency = 50)
        public String handleInvoice(Job<OrderInvoice> job) {
            job.updateProgress(50);
            job.log("Invoice generated successfully");
            return "SUCCESS";
        }
    }
}
```

---

## 📊 6. Real-Time Observability & Management

1. **Bull-Board Dashboard (Port 3000)**: Open `http://localhost:3000` to inspect queues, retry failed jobs, and view live step logs.
2. **Prometheus & Grafana (Port 3001)**: Open `http://localhost:3001` (login: `admin` / `admin`) to monitor throughput, error rates, and p99 latency percentiles.

---

## 📖 Deep-Dive Guides

* 🌲 [Parent-Child DAG Workflows Guide](DAG_WORKFLOWS.md)
* ⚡ [High-Throughput Batch Dequeue Guide](BATCH_INGESTION.md)
* ⏱️ [Sliding-Window Rate Limiting Guide](RATE_LIMITING.md)
* 🍃 [Spring Boot 3 Deep-Dive](SPRING_BOOT.md)
* 📊 [Observability & Telemetry Guide](OBSERVABILITY.md)
* ⚖️ [OxMQ vs BullMQ, JobRunr, Quartz & Kafka Comparison](COMPARISON.md)
