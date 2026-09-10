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
| `attemptsMade`| `int` | Number of times the job has been executed so far. |
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
    
    // Housekeeping & TTL
    .removeOnComplete(1000)                  // Keep last 1,000 completed jobs in Redis
    .removeOnFail(5000)                      // Keep last 5,000 failed jobs in Redis
    .build();
```

---

## 📈 Real-Time Progress & Execution Logs

Workers can stream real-time progress and emit diagnostic logs directly from the execution loop:

```java
OxmqWorker<TranscodeRequest> worker = OxmqWorker.<TranscodeRequest>builder()
    .queueName("video-transcoder")
    .jedisPool(jedisPool)
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
