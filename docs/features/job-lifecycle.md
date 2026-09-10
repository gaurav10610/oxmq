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

## 🛠️ Dynamic Job Operations

OxMQ supports dynamic in-flight job modifications:

```java
// Check current state across all Redis sets
JobState state = queue.getState(jobId);

// Dynamically adjust delay
queue.changeDelay(jobId, Duration.ofMinutes(10));

// Change priority on a waiting job
queue.changePriority(jobId, 10);

// Promote a delayed job immediately to WAITING
queue.promote(jobId);

// Retrieve execution logs
List<String> logs = queue.getJobLogs(jobId, 0, 50);
```
