# 🍃 Spring Boot 3.x Starter Guide
### *Declarative Distributed Jobs & DAG Workflows for Spring Boot 3*

`oxmq-spring-boot-starter` delivers zero-configuration integration for Spring Boot 3 microservices with declarative `@OxmqListener` annotations, automated Virtual Thread dispatching, Spring Boot Actuator health checks, and native Micrometer telemetry.

---

## 📦 1. Installation

### Maven (`pom.xml`)
```xml
<repositories>
    <repository>
        <id>jitpack.io</id>
        <url>https://jitpack.io</url>
    </repository>
</repositories>

<dependency>
    <groupId>com.github.gaurav10610.oxmq</groupId>
    <artifactId>oxmq-spring-boot-starter</artifactId>
    <version>v1.0.0</version>
</dependency>
```

### Gradle (`build.gradle.kts`)
```kotlin
repositories {
    mavenCentral()
    maven { url = uri("https://jitpack.io") }
}

dependencies {
    implementation("com.github.gaurav10610.oxmq:oxmq-spring-boot-starter:v1.0.0")
}
```

---

## ⚙️ 2. Configuration Properties Reference (`application.yml`)

```yaml
oxmq:
  # Redis Connection URI (supports standalone, Sentinel, and Cluster)
  redis:
    uri: ${OXMQ_REDIS_URI:redis://localhost:6379}
  
  # Default concurrency per worker queue
  default-concurrency: 50
  
  # Enable Java 21 Project Loom Virtual Threads (true by default)
  virtual-threads: true
  
  # Automatically bind timers, counters, and gauges to Micrometer MeterRegistry
  metrics-enabled: true

# Expose Actuator Endpoints
management:
  endpoints:
    web:
      exposure:
        include: "health,info,metrics,prometheus"
  endpoint:
    health:
      show-details: always
```

---

## 🛠️ 3. Declaring Strongly-Typed Queue Beans

Declare your strongly-typed `OxmqQueue` beans in any `@Configuration` class:

```java
import io.oxmq.OxmqQueue;
import io.oxmq.client.RedisConnectionManager;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class QueueConfig {

    @Bean
    public OxmqQueue<OrderEvent> orderQueue(RedisConnectionManager connectionManager) {
        return OxmqQueue.<OrderEvent>builder()
                .name("order-events")
                .connectionManager(connectionManager)
                .payloadClass(OrderEvent.class)
                .build();
    }
}
```

---

## 🎧 4. Declarative Worker Methods (`@OxmqListener`)

Annotate any Spring bean method with `@OxmqListener` to automatically register a background worker executing on Java 21 Virtual Threads:

```java
import io.oxmq.model.Job;
import io.oxmq.spring.annotation.OxmqListener;
import org.springframework.stereotype.Component;

@Component
public class OrderProcessingService {

    @OxmqListener(
        queue = "order-events",
        concurrency = 25,
        rateLimitMax = 100,
        rateLimitDurationMs = 60000
    )
    public OrderResult processOrder(Job<OrderEvent> job) {
        OrderEvent event = job.getData();
        job.updateProgress(20);
        
        // Blocking payment API call - runs on Virtual Thread!
        PaymentConfirmation confirmation = paymentGateway.charge(event.amount());
        job.updateProgress(80);
        
        return new OrderResult(event.orderId(), "COMPLETED", confirmation.id());
    }
}
```

---

## 🏥 5. Actuator Health Indicator

OxMQ automatically registers an `OxmqHealthIndicator` with Spring Boot Actuator.

Access `GET /actuator/health`:
```json
{
  "status": "UP",
  "components": {
    "oxmq": {
      "status": "UP",
      "details": {
        "redis": "Connected (PONG)",
        "activeWorkers": 2,
        "queues": ["order-events", "sync-orchestration-queue"]
      }
    }
  }
}
```

---

## 📊 6. Micrometer & Prometheus Metrics

All OxMQ workers automatically publish metrics to the Spring `MeterRegistry`:
- `oxmq.jobs.enqueued`: Total jobs added to queue.
- `oxmq.jobs.completed`: Total successfully completed jobs.
- `oxmq.jobs.failed`: Total failed jobs.
- `oxmq.jobs.active`: Current active jobs running in Virtual Threads.
- `oxmq.job.duration`: Execution duration timer with `p50`, `p95`, `p99` percentiles.
