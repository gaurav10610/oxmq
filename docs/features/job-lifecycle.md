# Job Lifecycle & States

Every job in OxMQ progresses through an atomic, strictly validated state machine governed directly by official BullMQ Redis Lua scripts.

---

## 🔄 State Machine Overview

<p align="center">
  <img src="/assets/oxmq-job-lifecycle.gif" alt="OxMQ Job Lifecycle Animation" style="border-radius: 8px; box-shadow: 0 4px 12px rgba(0,0,0,0.1); width: 100%;">
</p>

### The 8 Job States

| State | Redis Structure | Description |
| :--- | :--- | :--- |
| **`WAITING`** | `bull:<queue>:wait` (List) | Ready to be picked up by an available worker. |
| **`ACTIVE`** | `bull:<queue>:active` (List) | Currently locked and being executed by a worker Virtual Thread. |
| **`DELAYED`** | `bull:<queue>:delayed` (ZSet) | Scheduled to execute at a future epoch timestamp. |
| **`WAITING_CHILDREN`** | `bull:<queue>:waiting-children` (ZSet) | Parent node in a DAG flow waiting for all child jobs to complete. |
| **`COMPLETED`** | `bull:<queue>:completed` (ZSet) | Successfully finished execution; result is persisted in the job hash. |
| **`FAILED`** | `bull:<queue>:failed` (ZSet) | Exhausted all retry attempts or encountered an `UnrecoverableError`. |
| **`PAUSED`** | `bull:<queue>:paused` (List) | Queue processing is paused; jobs accumulate here without being claimed. |
| **`STALLED`** | `bull:<queue>:stalled` (ZSet) | Worker crashed or lost network connectivity; lock expired. |

---

## ⏱️ Stalled Job Recovery

When a worker claims a job, it acquires an atomic lock in Redis with a configurable TTL (e.g. 30 seconds).

1. **Heartbeat Lock Extension**: As long as the worker Virtual Thread is executing, OxMQ's background watchdog periodically extends the lock using `extendLock-2.lua`.
2. **Crash Detection**: If the worker process abruptly crashes (SIGKILL, hardware failure, OutOfMemory), the lock expires.
3. **Stalled Sentinel**: Another active worker running `moveStalledJobsToWait-9.lua` automatically identifies the orphaned job and moves it back to the `WAITING` queue to be safely reprocessed.

---

## 🔁 Retries & Backoff Strategies

Jobs can configure automatic retries with customized backoffs via `JobOptions`:

```java
JobOptions options = JobOptions.builder()
    .attempts(5) // Retry up to 5 times
    .exponentialBackoff(
        Duration.ofSeconds(1),  // Initial backoff
        Duration.ofMinutes(2)   // Maximum backoff cap
    )
    .build();

queue.add("transcode-video", payload, options);
```

When a job fails, OxMQ calculates the next attempt time and moves the job into the `DELAYED` sorted set via `retryJob-11.lua`. Once the backoff period expires, Redis promotes it back to `WAITING`.

---

## 🛠️ Dynamic Job Operations & Lifecycle Inspection

OxMQ supports dynamic in-flight job modifications and state inspection:

```java
// Check typed lifecycle state
JobState state = queue.getJobState(jobId);

// Fast atomic completion checks via isFinished-3.lua
boolean completed = queue.isCompleted(jobId);
boolean failed = queue.isFailed(jobId);

// Inspect loaded job
Job<MyPayload> job = queue.getJob(jobId);
if (job != null) {
    JobState s = job.getState();
    boolean isDone = job.isCompleted();
}

// Dynamically adjust delay
queue.changeDelay(jobId, Duration.ofMinutes(10));

// Change priority on a waiting job
queue.changePriority(jobId, 10);

// Promote a delayed job immediately to WAITING
queue.promote(jobId);

// Retrieve execution logs
List<String> logs = queue.getJobLogs(jobId, 0, 50);
```

---

## 🗑️ Retention Policies (`KeepJobs`)

Redis memory is valuable. BullMQ and OxMQ allow you to configure granular retention rules for completed and failed jobs so that Redis trims old jobs automatically without external cleanup cron jobs:

```java
// 1. Remove immediately upon successful completion
JobOptions removeImmediate = JobOptions.builder()
    .removeOnComplete(true)
    .build();

// 2. Keep at most the latest 500 completed jobs
JobOptions keepCount = JobOptions.builder()
    .removeOnComplete(500)
    .build();

// 3. Keep at most 1,000 jobs OR jobs newer than 7 days
JobOptions retention = JobOptions.builder()
    .removeOnComplete(KeepJobs.of(1000, Duration.ofDays(7)))
    .removeOnFail(KeepJobs.count(5000)) // Keep up to 5,000 failed jobs for debugging
    .build();

queue.add("process-order", orderData, retention);
```

---

## ⏳ Request-Reply: Waiting for Job Completion (`waitUntilFinished`)

In modern microservices, you often want to enqueue an asynchronous background job and await its final result synchronously or reactively (e.g. inside a REST controller or RPC gateway):

```java
// Enqueue the job
Job<InvoiceRequest> job = queue.add("generate-invoice", request);

// 1. Synchronously await result with timeout (blocks calling thread/virtual thread)
Object result = job.waitUntilFinished(queue.getQueueEvents(), Duration.ofSeconds(10));

// 2. Asynchronously await result via CompletableFuture
CompletableFuture<Object> future = job.waitUntilFinishedAsync(queue.getQueueEvents());
future.thenAccept(res -> log.info("Invoice generated: {}", res));

// 3. Or directly from OxmqTemplate in Spring Boot
Object templateResult = oxmqTemplate.waitUntilFinished("invoices", job.getId(), Duration.ofSeconds(10));
```

> ⚡ **Zero Race Conditions**: Under the hood, OxMQ checks `isFinished-3.lua` first. If the job finished before the listener was attached, the result is returned immediately without waiting for the timeout!

