# QueueEvents & Real-Time Streams

OxMQ provides **`OxmqQueueEvents`**, a non-blocking real-time event listener backed by **Redis Streams** (`XADD` / `XREAD`). It allows applications to listen to state transitions, progress updates, and completion notifications across any queue without polling.

---

## 📡 Redis Streams Architecture

Unlike traditional Pub/Sub which drops messages if a subscriber is briefly disconnected, Redis Streams persist event records in a fast append-only stream (`bull:<queue>:events`).

```text
[OxMQ Worker / Producer] 
       │
       ▼ (XADD via BullMQ Lua)
[Redis Stream: bull:<queue>:events]
       │
       ▼ (XREAD blocking read)
[OxmqQueueEvents Listener] ────> Dispatches callback on Virtual Thread!
```

---

## 💻 Listening to Events

Create an `OxmqQueueEvents` instance and register callbacks:

```java
package com.example;

import io.oxmq.QueueEvents;
import java.time.Duration;

public class EventNotificationService {

    public static void main(String[] args) throws Exception {
        QueueEvents events = QueueEvents.builder()
                .queueName("orders")
                .redisUri("redis://localhost:6379")
                .build()
                .start();

        // Job placed in wait queue
        events.onWaiting(jobId -> {
            System.out.println("⏳ Job queued: " + jobId);
        });

        // In-flight progress update (e.g. 25%, 50%, 75%)
        events.onProgress((jobId, progress) -> {
            System.out.printf("📈 Job %s progress: %s%%%n", jobId, progress);
        });

        // Job successfully completed
        var completedListener = (java.util.function.BiConsumer<String, Object>) (jobId, returnvalue) -> {
            System.out.println("✅ Job completed: " + jobId + ", output: " + returnvalue);
        };
        events.onCompleted(completedListener);

        // Job failed (exhausted retries or non-recoverable error)
        events.onFailed((jobId, failedReason) -> {
            System.err.println("❌ Job failed: " + jobId + ", reason: " + failedReason);
        });

        // Unregister listener when no longer needed
        events.off("completed", (jobId, payload) -> {});

        // Await job finish synchronously or reactively
        Object result = events.waitUntilFinished("job-123", Duration.ofSeconds(10));
        System.out.println("Job 123 finished with result: " + result);

        // Register shutdown hook
        Runtime.getRuntime().addShutdownHook(new Thread(events::close));
    }
}
```

---

## 📋 Available Event Callbacks

| Event Method | Trigger Event | Arguments |
| :--- | :--- | :--- |
| `onWaiting(consumer)` | Job is placed in `WAITING` queue. | `String jobId` |
| `onActive(consumer)` | Worker acquires job lock and begins processing. | `String jobId` |
| `onProgress(biConsumer)` | Worker invokes `job.updateProgress(...)`. | `String jobId, Object progress` |
| `onCompleted(biConsumer)` | Job finishes successfully. | `String jobId, String returnvalue` |
| `onFailed(biConsumer)` | Job exhausted retries or threw `UnrecoverableError`. | `String jobId, String failedReason` |
| `onStalled(consumer)` | Worker crash detected; job returned to wait queue. | `String jobId` |
| `onDrained(runnable)` | Queue has no remaining waiting or delayed jobs. | None |
| `onCleaned(biConsumer)` | Expired jobs were purged via `queue.clean(...)`. | `int count, String state` |

---

## 🧵 Concurrency & Non-Blocking Dispatch

Every incoming stream event is dispatched onto an independent **Java 21 Virtual Thread** (with automatic fallback to platform thread pools on Java 17), ensuring slow event handlers (such as sending WebSockets notifications or updating UI dashboards) never block the stream reader thread.

---

## 🧠 Stream Trimming & Connection Lifecycle

- **Approximate Stream Trimming (`maxLenEvents`)**: To prevent Redis memory exhaustion, stream emissions are capped using Redis approximate trimming (`XADD ... MAXLEN ~ 10000`). This keeps event history bounded to recent events with near $O(1)$ write efficiency.
- **Dedicated Poller Socket Isolation**: Because `xread(block(...))` holds a blocking network connection awaiting new events, `QueueEvents` isolates the blocking poller onto a dedicated connection with guaranteed `try ... finally { conn.close(); }` cleanup upon `.close()`.

*For complete operational tuning, see the [Memory Optimization & Resource Efficiency Guide](/operations/memory-optimization).*
