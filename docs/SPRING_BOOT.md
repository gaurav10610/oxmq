# 🍃 Spring Boot 3.x Starter Guide

`oxmq-spring-boot-starter` provides zero-config integration for Spring Boot 3 microservices with declarative listener annotations, Actuator health checks, and Micrometer telemetry.

---

## 📦 1. Add Dependency

```xml
<dependency>
    <groupId>io.oxmq</groupId>
    <artifactId>oxmq-spring-boot-starter</artifactId>
    <version>1.0.0-SNAPSHOT</version>
</dependency>
```

---

## ⚙️ 2. Configuration Properties Reference (`application.yml`)

```yaml
oxmq:
  # Redis Connection URI
  redis:
    uri: ${OXMQ_REDIS_URI:redis://localhost:6379}
  
  # Default concurrency for workers (Virtual Threads)
  default-concurrency: 50
  
  # Enable Java 21 Project Loom Virtual Threads
  virtual-threads: true
  
  # Enable Micrometer metric binding
  metrics-enabled: true

# Spring Boot Actuator Endpoints
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

## 🛠️ 3. Declaring Queues as Spring Beans

Declare your strongly-typed `OxmqQueue` beans in any `@Configuration` class:

```java
@Configuration
public class QueueConfig {

    @Bean
    public OxmqQueue<OrderEvent> orderQueue(RedisClient redisClient) {
        return OxmqQueue.<OrderEvent>builder()
                .name("order-events")
                .redisClient(redisClient)
                .payloadClass(OrderEvent.class)
                .build();
    }
}
```

---

## 👂 4. Declarative Consumers with `@OxmqListener`

Annotate any Spring bean method with `@OxmqListener`:

```java
@Component
public class OrderProcessingService {

    @OxmqListener(queue = "order-events", concurrency = 100)
    public String processOrder(Job<OrderEvent> job) {
        OrderEvent event = job.getData();
        job.updateProgress(50);
        job.log("Processing order: " + event.orderId());
        
        // Execute business logic (e.g. database updates, payment calls)
        return "ORDER_PROCESSED";
    }
}
```

### Supported Method Signatures
- `void process(Job<T> job)`
- `String process(Job<T> job)` (return value is serialized as job result)
- `Map<String, Object> process(Job<T> job)`

---

## 🩺 5. Actuator Health & Metrics

### Health Endpoint (`/actuator/health`)
When `oxmq-spring-boot-starter` is present, it registers an `OxmqHealthIndicator` validating connectivity to Redis:

```json
{
  "status": "UP",
  "components": {
    "oxmq": {
      "status": "UP",
      "details": {
        "redis": "PONG",
        "engine": "OxMQ v1.0.0 (Virtual Threads Enabled)"
      }
    }
  }
}
```

### Metrics Endpoint (`/actuator/metrics`)
Query OxMQ metrics via Actuator:
* `/actuator/metrics/oxmq.jobs.completed`
* `/actuator/metrics/oxmq.jobs.failed`
* `/actuator/metrics/oxmq.job.duration`
