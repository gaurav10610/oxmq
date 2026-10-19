# 🍃 OxMQ Spring Boot 3+ Starter (`oxmq-spring-boot-starter`)

<p align="center">
  <a href="https://central.sonatype.com/artifact/io.github.gaurav10610/oxmq-spring-boot-starter"><img src="https://img.shields.io/badge/Maven%20Central-v1.6.0-C71A36.svg" alt="Maven Central v1.6.0"></a>
  <a href="https://github.com/gaurav10610/oxmq/releases/tag/v1.6.0"><img src="https://img.shields.io/badge/Release-v1.6.0-blue.svg" alt="Release v1.6.0"></a>
  <a href="https://jitpack.io/#gaurav10610/oxmq"><img src="https://img.shields.io/badge/JitPack-v1.6.0-brightgreen.svg" alt="JitPack"></a>
  <a href="https://spring.io/projects/spring-boot"><img src="https://img.shields.io/badge/Spring%20Boot-3.x-6DB33F.svg" alt="Spring Boot 3"></a>
  <a href="https://openjdk.org/projects/jdk/17/"><img src="https://img.shields.io/badge/Java-17%2B%20%7C%2021%2B%20LTS-orange.svg" alt="Java 17+ and 21+ LTS"></a>
</p>

> First-class, **100% annotation-driven autoconfiguration** for Spring Boot 3 microservices on **Java 17 LTS and Java 21+ (Project Loom Virtual Threads)**.

---

## 📦 Maven / Gradle Dependency

### Maven Central (Global Standard — Zero Config)
```xml
<dependency>
    <groupId>io.github.gaurav10610</groupId>
    <artifactId>oxmq-spring-boot-starter</artifactId>
    <version>1.6.0</version>
</dependency>
```

### Gradle
```kotlin
implementation("io.github.gaurav10610:oxmq-spring-boot-starter:1.6.0")
```

---

## 🚀 60-Second Setup

### 1. Enable OxMQ in your Application
```java
@SpringBootApplication
@EnableOxmq
public class OrderApplication {
    public static void main(String[] args) {
        SpringApplication.run(OrderApplication.class, args);
    }
}
```

### 2. Configure `application.yml`
```yaml
spring:
  application:
    name: order-service             # Inherited automatically as context isolation namespace!

oxmq:
  redis:
    uri: redis://localhost:6379
  default-concurrency: 50          # 50 Virtual Threads per worker
  virtual-threads: true            # Java 21 Loom (auto platform fallback on Java 17)
  metrics-enabled: true            # Native Micrometer telemetry
  default-job-options:
    remove-on-complete:
      count: 1000                  # Keep latest 1,000 completed jobs
      age-seconds: 86400           # Purge completed jobs older than 24h
    remove-on-fail:
      count: 5000                  # Retain up to 5,000 failed jobs for debugging
      age-seconds: 604800          # Purge failed jobs older than 7 days
```

---

## 🎧 Declarative Listeners & Annotations

### 1. `@OxmqListener` (Single-Job Virtual Thread Consumer)
```java
@Component
public class PaymentWorker {

    @OxmqListener(
        queue = "${app.queues.payments:payments}",
        concurrencyString = "${app.concurrency.payments:50}",
        rateLimitMax = 100,
        rateLimitDurationMs = 1000
    )
    public PaymentResult handlePayment(
            Job<PaymentPayload> job,
            @Header("jobId") String jobId,
            @Header("timestamp") long timestamp
    ) {
        job.updateProgress(50);
        return paymentService.charge(job.getData());
    }
}
```

### 2. `@OxmqBatchListener` (Atomic Bulk Dequeue)
```java
@Component
public class AuditBatchWorker {

    @OxmqBatchListener(
        queue = "audit-events",
        batchSize = 100,
        batchTimeoutMs = 200,
        maxBatchBytes = 16 * 1024 * 1024
    )
    public void ingestAuditBatch(List<Job<AuditEvent>> batch) {
        clickHouseService.batchInsert(batch.stream().map(Job::getData).toList());
    }
}
```

### 3. Multi-Event Class Routing (`@OxmqListener` + `@OxmqHandler`)
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

## 🔌 Automatic Plugin Bean Discovery

The starter automatically discovers all `JobInterceptor` and OxMQ plugin beans registered in the Spring `ApplicationContext` and binds them to all workers:

```java
@Configuration
public class OxmqPluginsConfig {

    @Bean
    public AdaptiveRateLimiterPlugin adaptiveLimiter(RedisConnectionManager cm) {
        return AdaptiveRateLimiterPlugin.builder()
                .connectionManager(cm)
                .groupKeyResolver(job -> ((TenantScoped) job.getData()).tenantId())
                .build();
    }

    @Bean
    public CheckpointStatePlugin checkpointPlugin(RedisConnectionManager cm) {
        return CheckpointStatePlugin.builder()
                .connectionManager(cm)
                .namespace("m365-sync")
                .groupKeyResolver(job -> ((TenantScoped) job.getData()).tenantId())
                .build();
    }
}
```

---

## 📊 Actuator Health & Bundled Grafana Dashboard

### Health Indicator
OxMQ registers an Actuator health indicator:
`GET /actuator/health`
```json
{
  "status": "UP",
  "components": {
    "oxmq": {
      "status": "UP",
      "details": {
        "activeWorkers": 3,
        "redisConnection": "CONNECTED",
        "redisUri": "redis://localhost:6379"
      }
    }
  }
}
```

### Bundled Grafana Dashboard
A production-ready Grafana dashboard template is packaged inside:
`src/main/resources/dashboards/oxmq-grafana-dashboard.json`.
Import directly into Grafana with Prometheus datasource to monitor queue backlogs, throughput, and latencies.
