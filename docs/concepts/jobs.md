# Jobs

A **Job** represents an individual unit of work in OxMQ. Jobs carry user-defined payloads, execution options, lifecycle states, progress telemetry, step logs, and return values.

---

## 🧬 Job Anatomy

In Redis, every job is stored as a hash under the key `bull:<queue>:<jobId>`:

| Field | Type | Description |
| :--- | :--- | :--- |
| `id` | `String` | Unique identifier (auto-generated or custom-provided). |
| `name` | `String` | Logical job name (e.g. `process-payment`, `send-email`). |
| `data` | `JSON String` | Serialized payload object. |
| `opts` | `JSON String` | Serialized `JobOptions` (delays, retries, backoff parameters). |
| `timestamp` | `long` | Epoch millisecond timestamp when the job was enqueued. |
| `processedOn` | `long` | Timestamp when a worker claimed the job and entered `ACTIVE`. |
| `finishedOn` | `long` | Timestamp when the job finished execution (`COMPLETED` or `FAILED`). |
| `attemptsMade`| `int` | Number of times the job has finished an execution attempt. |
| `attemptsStarted`| `int` | Number of times a worker has picked up the job to start executing. |
| `returnvalue` | `JSON String` | Value returned by the worker upon successful completion. |
| `failedReason`| `String` | Error message or exception description if the job failed. |
| `stacktrace` | `JSON Array` | Full Java exception stack trace for debugging. |
| `progress` | `Number / JSON` | Real-time execution progress (e.g. `0` to `100`). |

---

## ⚙️ `JobOptions` Reference

Customize job execution behavior using the `JobOptions` builder:

```java
JobOptions options = JobOptions.builder()
    // Execution Timing
    .delay(Duration.ofSeconds(30))           // Wait 30s before becoming available
    
    // Retries & Resilience
    .attempts(5)                             // Retry up to 5 times on exception
    .exponentialBackoff(                     // Exponential backoff strategy
        Duration.ofSeconds(2),               // Initial backoff: 2s, 4s, 8s, 16s...
        Duration.ofMinutes(5)                // Max backoff cap: 5m
    )
    
    // Scheduling & Priority
    .priority(1)                             // Lower number = higher execution priority
    .lifo(false)                             // False = FIFO (queue), True = LIFO (stack)
    
    // Deduplication & Debounce
    .jobId("tx_987654")                      // Custom deterministic ID (prevents duplicates)
    .deduplicationId("checkout_user_42")     // Custom deduplication key
    .debounceDuration(Duration.ofSeconds(5)) // Resets execution timer on rapid submissions
    
    // Multi-Tenant Partitioning
    .groupKey("tenant_acme")                 // Partition rate limit by tenant
    
    // Housekeeping & Redis Memory Pruning (KeepJobs)
    .removeOnComplete(KeepJobs.of(1000, Duration.ofHours(2))) // Retain last 1,000 completed jobs or < 2 hours
    .removeOnFail(KeepJobs.age(Duration.ofDays(7)))           // Purge failed jobs older than 7 days
    
    // DAG Workflow Policies
    .failParentOnFailure(true)                 // If child fails, immediately fail parent DAG job
    .removeDependencyOnFailure(false)          // If true, unlink failed child so parent can still run
    .build();
```

---

## 🚦 Standardized Lifecycle States (`JobState`)

OxMQ models the complete lifecycle of a job via the **`JobState`** enum:

| State | BullMQ Wire Value | Description |
| :--- | :--- | :--- |
| `WAITING` | `"waiting"` | Job is queued in `wait` list, ready to be claimed by an available worker. |
| `ACTIVE` | `"active"` | Worker has claimed the job and locked it; Virtual Thread is executing task. |
| `DELAYED` | `"delayed"` | Scheduled for future execution via Redis sorted set timestamps. |
| `WAITING_CHILDREN` | `"waiting-children"` | Parent DAG job waiting for dependent child tasks to complete. |
| `COMPLETED` | `"completed"` | Successfully processed, storing its return value in Redis. |
| `FAILED` | `"failed"` | Exhausted all retries or failed with a non-recoverable error. |
| `PAUSED` | `"paused"` | Queue is paused; waiting jobs accumulate safely in Redis without dispatch. |

### Inspecting Job State Programmatically:
```java
// Inspect state via Job instance
JobState state = job.getState();

// Or inspect via Queue by jobId
JobState stateFromQueue = queue.getJobState(job.getId());

// Ergonomic helper predicates:
if (state.isCompleted()) {
    log.info("Job successfully completed with output: {}", job.getReturnvalue());
} else if (state.isFailed()) {
    log.error("Job failed: {}", job.getFailedReason());
} else if (state.isActive()) {
    log.info("Job is currently in-flight on a worker");
}
```

---

## ⏱️ Request-Reply: Awaiting Completion (`waitUntilFinished`)

A producer or controller can await the final result of a background job directly from the `Job` instance:

### Synchronous Virtual-Thread Safe Waiting:
```java
Job<OrderPayload> job = queue.add("checkout", order);

try {
    // Blocks the calling thread (unmounts carrier thread on Java 21 Loom)
    Object result = job.waitUntilFinished(queueEvents, Duration.ofSeconds(10));
    System.out.println("Job completed with result: " + result);
} catch (TimeoutException e) {
    System.err.println("Timed out waiting for worker");
} catch (ExecutionException e) {
    System.err.println("Worker execution failed: " + e.getCause().getMessage());
}
```

### Asynchronous / Non-Blocking:
```java
CompletableFuture<Object> future = job.waitUntilFinishedAsync(queueEvents, Duration.ofSeconds(30));
future.thenAccept(result -> log.info("Async result received: {}", result));
```

---

## 📈 Real-Time Progress & Execution Logs

Workers can stream real-time progress and emit diagnostic logs directly from the execution loop:

```java
OxmqWorker<TranscodeRequest> worker = OxmqWorker.<TranscodeRequest>builder()
    .queueName("video-transcoder")
    .redisUri("redis://localhost:6379")
    .processor(job -> {
        TranscodeRequest request = job.getData();

        // 1. Log step messages (visible in Bull-Board & queryable via API)
        job.log("Starting FFmpeg pass 1 of 2...");

        for (int frame = 1; frame <= 100; frame++) {
            renderFrame(frame);

            // 2. Report progress percentage (0 to 100)
            if (frame % 10 == 0) {
                job.updateProgress(frame);
            }
        }

        job.log("Transcoding completed successfully.");
        return new TranscodeResult("/videos/" + request.id() + ".mp4", 1080);
    })
    .build();
```

### Querying Logs from Queue API:

```java
List<String> logs = queue.getJobLogs(jobId);
logs.forEach(System.out::println);
```

---

## 🔁 Automatic Retries & Backoff Calculation

When an exception is thrown inside a worker processor:
1. OxMQ checks if `attemptsMade < attempts`.
2. If retries remain:
   - **Fixed Backoff**: Delays the next attempt by the configured fixed duration.
   - **Exponential Backoff**: Delays the next attempt by `initialDelay * 2^(attemptsMade - 1)`, capped at `maxDelay`.
3. The job is placed into `bull:<queue>:delayed` with a calculated maturity timestamp score.
4. When the timestamp arrives, Redis automatically promotes the job back to `bull:<queue>:wait`.
5. If all attempts are exhausted, the job moves to `bull:<queue>:failed`.

---

## 🚫 Non-Recoverable Errors (`UnrecoverableError`)

If an exception indicates a fatal condition that will never succeed on retry (e.g. malformed payload, account suspended), throw `UnrecoverableError`:

```java
import io.oxmq.exception.UnrecoverableError;

if (user.isSuspended()) {
    throw new UnrecoverableError("User account suspended. Bypassing retries.");
}
```

OxMQ catches `UnrecoverableError` and immediately moves the job to `FAILED`, bypassing remaining attempts.

---

## ⏸️ Worker-Driven Postponement (`DelayedException`)

In BullMQ v6, a worker can postpone execution without consuming an attempt count or marking the attempt as failed. In OxMQ, simply throw `DelayedException`:

```java
import io.oxmq.exception.DelayedException;

if (!paymentGatewayReady()) {
    // Postpone execution for 10 seconds without incrementing attemptsMade!
    throw new DelayedException(Duration.ofSeconds(10));
}
```

Behind the scenes, OxMQ catches `DelayedException` and executes BullMQ's `moveToDelayed-11.lua` with `skipAttempt: true`. The job transitions back to `DELAYED` without exhausting attempts or recording failure reasons.

---

## 🛑 Dynamic Job Parking (`WaitingException` & `WaitingChildrenException`)

Workers can dynamically yield and park execution when waiting for external conditions or dependent child trees:

- **`WaitingException`**: Moves the job back to `WAITING` state to be retried after other queued jobs.
- **`WaitingChildrenException`**: Parks a job in `WAITING_CHILDREN` until dependencies complete.
