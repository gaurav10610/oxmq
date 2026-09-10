# Spring Boot 3+ Starter

The `oxmq-spring-boot-starter` module provides full auto-configuration, declarative queue listeners via `@OxmqListener`, and automated Spring Boot Actuator telemetry.

---

## 📦 Dependency

::: code-group

```xml [Maven (pom.xml)]
<dependency>
    <groupId>com.github.gaurav10610.oxmq</groupId>
    <artifactId>oxmq-spring-boot-starter</artifactId>
    <version>v1.0.0</version>
</dependency>
```

```kotlin [Gradle (build.gradle.kts)]
implementation("com.github.gaurav10610.oxmq:oxmq-spring-boot-starter:v1.0.0")
```

:::

---

## ⚙️ Configuration (`application.yml`)

Configure Redis connection, thread concurrency, and stalled-job detection in `application.yml`:

```yaml
oxmq:
  redis:
    host: localhost
    port: 6379
    timeout-ms: 2000
    pool:
      max-total: 64
      max-idle: 32
      min-idle: 8
  worker:
    virtual-threads: true           # Enable Java 21 Loom Virtual Threads
    default-concurrency: 50         # Default concurrent workers per queue
    stalled-interval-ms: 30000      # Check for stalled/crashed workers every 30s
    lock-duration-ms: 30000         # Worker heartbeat lock duration
```

---

## 🎧 Declarative Listeners (`@OxmqListener`)

Annotate any Spring `@Component` method with `@OxmqListener`. OxMQ inspects method parameter types and automatically deserializes the incoming JSON payload into your record or class:

```java
package com.example.workers;

import io.oxmq.model.Job;
import io.oxmq.spring.annotation.OxmqListener;
import org.springframework.stereotype.Component;

public record EmailNotification(String to, String subject, String body) {}

@Component
public class NotificationWorker {

    @OxmqListener(queue = "notifications", concurrency = 100)
    public void sendNotification(Job<EmailNotification> job) {
        EmailNotification data = job.getData();
        
        // Virtual threads handle blocking network operations seamlessly
        emailClient.send(data.to(), data.subject(), data.body());
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
        // Fails immediately and moves job to FAILED state without retrying
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
        "notifications": {
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
