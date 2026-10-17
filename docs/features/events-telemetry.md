# Events & Telemetry

OxMQ offers non-blocking real-time event streaming via Redis Streams and native Micrometer telemetry for production observability.

---

## 📡 Real-Time Queue Events (`QueueEvents`)

Listen to distributed queue events across all nodes without polling via Redis Streams (`XREAD BLOCK`):

```java
package com.example;

import io.oxmq.QueueEvents;
import java.time.Duration;

public class QueueEventsExample {
    public static void main(String[] args) throws Exception {
        QueueEvents events = QueueEvents.builder()
                .queueName("order-invoices")
                .redisUri("redis://localhost:6379")
                .build()
                .start();

        // 1. Reactive event callbacks
        events.onWaiting(jobId -> {
            System.out.println("⏳ Job queued: " + jobId);
        });

        events.onProgress((jobId, progress) -> {
            System.out.printf("📈 Job %s progress: %s%%%n", jobId, progress);
        });

        var completedListener = (java.util.function.BiConsumer<String, Object>) (jobId, returnvalue) -> {
            System.out.println("✅ Job completed: " + jobId + ", result: " + returnvalue);
        };
        events.onCompleted(completedListener);

        events.onFailed((jobId, failedReason) -> {
            System.err.println("❌ Job failed: " + jobId + ", reason: " + failedReason);
        });

        // 2. Unregister listener when no longer needed
        events.off("completed", (jobId, payload) -> {});

        // 3. Synchronously or reactively await job completion
        Object result = events.waitUntilFinished("job-123", Duration.ofSeconds(10));
        System.out.println("Finished with result: " + result);
    }
}
```

---

## 📊 Progress Reporting

Workers can report progress percentage or structured progress metadata in-flight:

```java
OxmqWorker<VideoPayload> worker = OxmqWorker.<VideoPayload>builder()
        .queueName("video-encoder")
        .redisUri("redis://localhost:6379")
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
