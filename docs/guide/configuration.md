# Configuration Reference

This guide provides a comprehensive reference for all configuration properties, builder options, and environment variables available in **OxMQ**.

---

## 🍃 Spring Boot Properties Reference (`application.yml`)

When using `oxmq-spring-boot-starter`, all properties are prefixed with `oxmq`:

| Property | Type | Default | Description |
| :--- | :--- | :--- | :--- |
| `oxmq.redis.uri` | `String` | `redis://localhost:6379` | Full Redis connection URI (supports `redis://` and `rediss://` for TLS). |
| `oxmq.redis.host` | `String` | `localhost` | Redis server hostname (used if `uri` is omitted). |
| `oxmq.redis.port` | `int` | `6379` | Redis server port. |
| `oxmq.redis.password` | `String` | `null` | Redis AUTH password (optional). |
| `oxmq.default-concurrency` | `int` | `20` | Default worker concurrency applied to `@OxmqListener` when unspecified. |
| `oxmq.virtual-threads` | `boolean` | `true` | Whether to execute worker tasks on Java 21 Virtual Threads (Project Loom). |
| `oxmq.metrics-enabled` | `boolean` | `true` | Enables native Micrometer telemetry for Prometheus and Spring Boot Actuator. |

### Example `application.yml`

```yaml
oxmq:
  redis:
    uri: redis://localhost:6379
    # Or separate fields:
    # host: redis-cluster.internal
    # port: 6379
    # password: ${REDIS_PASSWORD}
  default-concurrency: 50
  virtual-threads: true
  metrics-enabled: true

# Expose OxMQ Actuator health & metrics
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

## 🎧 `@OxmqListener` Annotation Attributes

Use `@OxmqListener` on any Spring `@Component` method to consume jobs:

| Attribute | Type | Default | Description |
| :--- | :--- | :--- | :--- |
| `queue` | `String` | *(Required)* | Name of the Redis queue to process. |
| `concurrency` | `int` | `20` | Number of concurrent jobs processed simultaneously on Virtual Threads. |
| `virtualThreads`| `boolean` | `true` | Whether to dispatch jobs to Virtual Threads or fallback to platform threads. |
| `lockDurationMs`| `long` | `30000` | Duration of the distributed worker lock in milliseconds (auto-extended by heartbeat). |
| `pollIntervalMs`| `long` | `50` | Sleep interval in milliseconds when no waiting jobs are available in Redis. |
| `rateLimitMax` | `int` | `0` | Max jobs allowed in the sliding time window (`0` means unlimited). |
| `rateLimitDurationMs` | `long` | `1000` | Sliding window duration in milliseconds for the rate limiter. |

---

## ☕ Standalone Java Builder Options

If you are using `oxmq-core` in a pure Java application without Spring:

### `OxmqQueue.Builder<T>`

```java
OxmqQueue<MyPayload> queue = OxmqQueue.<MyPayload>builder()
    .name("orders")                         // Required: Queue name
    .jedisPool(jedisPool)                   // Required: Jedis connection pool
    .payloadClass(MyPayload.class)          // Required: JSON serialization target class
    .prefix("bull")                         // Optional: Redis key prefix (default: "bull")
    .build();
```

### `OxmqWorker.Builder<T>`

```java
OxmqWorker<MyPayload> worker = OxmqWorker.<MyPayload>builder()
    .queueName("orders")                    // Required: Queue name
    .jedisPool(jedisPool)                   // Required: Jedis connection pool
    .payloadClass(MyPayload.class)          // Required: Payload target class
    .prefix("bull")                         // Optional: Key prefix (default: "bull")
    .concurrency(50)                        // Optional: Max concurrent jobs (default: 20)
    .useVirtualThreads(true)                // Optional: Enable Loom (default: true)
    .lockDuration(Duration.ofSeconds(30))   // Optional: Lock TTL (default: 30s)
    .stalledInterval(Duration.ofSeconds(30))// Optional: Check stalled workers interval
    .maxStalledCount(1)                     // Optional: Max times a stalled job can recover
    .processor(job -> {                     // Required: Job processing lambda
        return process(job.getData());
    })
    .build();
```

### `JobOptions.Builder`

```java
JobOptions options = JobOptions.builder()
    .delay(Duration.ofSeconds(10))          // Initial delay before becoming WAITING
    .attempts(5)                            // Max execution attempts
    .exponentialBackoff(
        Duration.ofSeconds(1),              // Initial backoff interval
        Duration.ofMinutes(5)               // Maximum backoff interval cap
    )
    .fixedBackoff(Duration.ofSeconds(5))    // Or use fixed backoff interval
    .priority(10)                           // Priority: lower number = higher priority
    .lifo(false)                            // True for LIFO (stack), false for FIFO (queue)
    .jobId("custom-unique-id")              // Optional: Custom job identifier
    .deduplicationId("user-7788-order")     // Custom deduplication key
    .debounceDuration(Duration.ofSeconds(3))// Debounce quiet window
    .groupKey("tenant-acme-corp")           // Group key for tenant-level rate limiting
    .removeOnComplete(1000)                 // Keep last 1,000 completed jobs in Redis
    .removeOnFail(5000)                     // Keep last 5,000 failed jobs in Redis
    .build();
```
