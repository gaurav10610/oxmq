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

import io.oxmq.events.OxmqQueueEvents;
import redis.clients.jedis.JedisPool;

public class EventNotificationService {

    public static void main(String[] args) {
        JedisPool jedisPool = new JedisPool("localhost", 6379);

        OxmqQueueEvents events = new OxmqQueueEvents("orders", jedisPool);

        // Job claimed by a worker
        events.onActive(jobId -> {
            System.out.println("⚡ Job claimed: " + jobId);
        });

        // In-flight progress update (e.g. 25%, 50%, 75%)
        events.onProgress((jobId, progress) -> {
            System.out.printf("📈 Job %s progress: %s%%%n", jobId, progress);
        });

        // Job successfully completed
        events.onCompleted((jobId, returnvalue) -> {
            System.out.println("✅ Job completed: " + jobId + ", output: " + returnvalue);
        });

        // Job failed (exhausted retries or non-recoverable error)
        events.onFailed((jobId, failedReason) -> {
            System.err.println("❌ Job failed: " + jobId + ", reason: " + failedReason);
        });

        // Worker stalled
        events.onStalled(jobId -> {
            System.err.println("⚠️ Worker stalled for job: " + jobId);
        });

        // Queue completely drained
        events.onDrained(() -> {
            System.out.println("🎉 Queue is empty!");
        });

        // Start listening in background
        events.start();

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

Every incoming stream event is dispatched onto an independent **Java 21 Virtual Thread**, ensuring slow event handlers (such as sending WebSockets notifications or updating UI dashboards) never block the stream reader thread.
