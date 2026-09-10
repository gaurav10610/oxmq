# Events & Telemetry

OxMQ offers non-blocking real-time event streaming via Redis Streams and native Micrometer telemetry for production observability.

---

## 📡 Real-Time Queue Events (`OxmqQueueEvents`)

Listen to distributed queue events across all nodes without polling:

```java
package com.example;

import io.oxmq.events.OxmqQueueEvents;
import redis.clients.jedis.JedisPool;

public class QueueEventsExample {
    public static void main(String[] args) {
        JedisPool jedisPool = new JedisPool("localhost", 6379);

        OxmqQueueEvents events = new OxmqQueueEvents("order-invoices", jedisPool);

        events.onActive(jobId -> {
            System.out.println("⚡ Job claimed and active: " + jobId);
        });

        events.onProgress((jobId, progress) -> {
            System.out.printf("📈 Job %s progress updated: %s%%%n", jobId, progress);
        });

        events.onCompleted((jobId, returnvalue) -> {
            System.out.println("✅ Job completed: " + jobId + ", result: " + returnvalue);
        });

        events.onFailed((jobId, failedReason) -> {
            System.err.println("❌ Job failed: " + jobId + ", reason: " + failedReason);
        });

        events.start();
    }
}
```

---

## 📊 Progress Reporting

Workers can report progress percentage or structured progress metadata in-flight:

```java
OxmqWorker<VideoPayload> worker = OxmqWorker.<VideoPayload>builder()
        .queueName("video-encoder")
        .jedisPool(jedisPool)
        .processor(job -> {
            for (int chunk = 1; chunk <= 10; chunk++) {
                encodeChunk(chunk);
                // Atomically update progress in Redis & emit stream event
                job.updateProgress(chunk * 10);
            }
            return "Encoding complete";
        })
        .build();
```

---

## 📈 Micrometer & Prometheus Metrics

OxMQ exports native metrics that integrate automatically with Spring Boot Actuator, Prometheus, Datadog, and Grafana:

| Metric Name | Type | Description |
| :--- | :--- | :--- |
| `oxmq.jobs.processed` | Counter | Total number of jobs successfully processed per queue. |
| `oxmq.jobs.failed` | Counter | Total number of jobs failed per queue. |
| `oxmq.jobs.duration` | Timer | Latency distribution of job execution times. |
| `oxmq.queue.size` | Gauge | Instantaneous count of waiting jobs in the queue. |
| `oxmq.workers.active` | Gauge | Number of active virtual threads executing tasks. |
