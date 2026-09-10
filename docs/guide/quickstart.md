# Quickstart (Core Engine)

Get up and running with OxMQ in a pure Java 21+ application in less than 5 minutes.

---

## 📋 Prerequisites

- **Java 21** or newer (OpenJDK, Temurin, Corretto, GraalVM)
- **Redis 6.2+**, **Redis 7.x**, or **Valkey** running locally or in Docker

```bash
# Start a clean Redis instance via Docker
docker run -d --name oxmq-redis -p 6379:6379 redis:7-alpine
```

---

## 📦 1. Add Dependency

::: code-group

```xml [Maven (pom.xml)]
<repositories>
    <repository>
        <id>jitpack.io</id>
        <url>https://jitpack.io</url>
    </repository>
</repositories>

<dependencies>
    <dependency>
        <groupId>com.github.gaurav10610.oxmq</groupId>
        <artifactId>oxmq-core</artifactId>
        <version>v1.0.0</version>
    </dependency>
</dependencies>
```

```kotlin [Gradle (build.gradle.kts)]
repositories {
    mavenCentral()
    maven { url = uri("https://jitpack.io") }
}

dependencies {
    implementation("com.github.gaurav10610.oxmq:oxmq-core:v1.0.0")
}
```

:::

---

## 📝 2. Define Your Payload

OxMQ works natively with Java 21 records and standard POJOs via Jackson:

```java
package com.example.model;

public record OrderInvoice(
    String orderId,
    String customerEmail,
    double amount
) {}
```

---

## 📤 3. Produce Jobs

Initialize a connection pool and enqueue jobs using `OxmqQueue`:

```java
package com.example;

import com.example.model.OrderInvoice;
import io.oxmq.OxmqQueue;
import io.oxmq.model.Job;
import io.oxmq.model.JobOptions;
import redis.clients.jedis.JedisPool;

import java.time.Duration;

public class OrderProducer {
    public static void main(String[] args) {
        JedisPool jedisPool = new JedisPool("localhost", 6379);

        // Build the Queue
        OxmqQueue<OrderInvoice> queue = OxmqQueue.<OrderInvoice>builder()
                .name("order-invoices")
                .jedisPool(jedisPool)
                .payloadClass(OrderInvoice.class)
                .build();

        // Enqueue with 5s delay, 3 retry attempts, and exponential backoff
        Job<OrderInvoice> job = queue.add(
                "generate-pdf",
                new OrderInvoice("ord_101", "alice@example.com", 149.50),
                JobOptions.builder()
                        .delay(Duration.ofSeconds(5))
                        .attempts(3)
                        .exponentialBackoff(Duration.ofSeconds(1), Duration.ofSeconds(30))
                        .build()
        );

        System.out.println("Enqueued job ID: " + job.getId());
    }
}
```

---

## 📥 4. Consume Jobs with Virtual Threads

Create an `OxmqWorker` to execute tasks concurrently using Java 21 Virtual Threads:

```java
package com.example;

import com.example.model.OrderInvoice;
import io.oxmq.OxmqWorker;
import redis.clients.jedis.JedisPool;

import java.time.Duration;

public class OrderWorker {
    public static void main(String[] args) throws InterruptedException {
        JedisPool jedisPool = new JedisPool("localhost", 6379);

        OxmqWorker<OrderInvoice> worker = OxmqWorker.<OrderInvoice>builder()
                .queueName("order-invoices")
                .jedisPool(jedisPool)
                .payloadClass(OrderInvoice.class)
                .concurrency(50) // 50 concurrent Virtual Threads
                .lockDuration(Duration.ofSeconds(30))
                .processor(job -> {
                    OrderInvoice invoice = job.getData();
                    System.out.printf("[%s] Processing order %s ($%.2f) on thread %s%n",
                            job.getId(), invoice.orderId(), invoice.amount(), Thread.currentThread());

                    // Virtual threads unmount during blocking I/O (DB, HTTP, Disk)
                    Thread.sleep(500); 

                    // Return any serializable result or null
                    return "Invoice PDF generated: /invoices/" + invoice.orderId() + ".pdf";
                })
                .build();

        worker.start();
        System.out.println("Worker started. Listening for jobs...");

        // Graceful shutdown on JVM exit
        Runtime.getRuntime().addShutdownHook(new Thread(worker::close));
    }
}
```

---

## ⚡ What Happens Under the Hood?

1. **Atomic Ingestion**: The producer invokes BullMQ's `addDelayedJob-6.lua` script, pushing the job into Redis with delayed maturity.
2. **Auto-Promotion**: BullMQ Lua scripts check the Redis sorted set `bull:order-invoices:delayed` and atomically move matured jobs to `bull:order-invoices:wait`.
3. **Virtual Thread Worker**: The `OxmqWorker` pulls ready jobs via `moveToActive-11.lua` and dispatches each job to an unpinned Java 21 Virtual Thread.
4. **Heartbeat Lock Watchdog**: An automated background timer extends the job lock via `extendLock-2.lua` until task completion.
5. **Completion & Cleanup**: `moveToFinished-14.lua` marks the job completed and persists the return value.
