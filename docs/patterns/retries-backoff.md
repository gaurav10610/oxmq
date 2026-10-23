# Retries & Backoff Policies

Transient failures (such as temporary network glitches, rate limit throttles, or database deadlocks) are normal in distributed environments. OxMQ provides configurable retry policies with automated backoff schedules.

---

## Exponential Backoff (Recommended)

Exponential backoff increases the delay exponentially between successive retry attempts to give overloaded downstream services time to recover:

```java
JobOptions options = JobOptions.builder()
    .attempts(5) // Max 5 total attempts
    .exponentialBackoff(
        Duration.ofSeconds(1),  // Initial backoff interval (1s)
        Duration.ofMinutes(5)   // Maximum backoff cap (5m)
    )
    .build();

queue.add("process-charge", paymentPayload, options);
```

### Delay Schedule:
- Attempt 1: Fails
- Attempt 2: After $1\text{s} \times 2^0 = 1\text{s}$
- Attempt 3: After $1\text{s} \times 2^1 = 2\text{s}$
- Attempt 4: After $1\text{s} \times 2^2 = 4\text{s}$
- Attempt 5: After $1\text{s} \times 2^3 = 8\text{s}$
- Exhausted $\rightarrow$ Moved to `bull:<queue>:failed`

---

## Fixed Backoff

If your workload requires a constant retry interval:

```java
JobOptions options = JobOptions.builder()
    .attempts(3)
    .fixedBackoff(Duration.ofSeconds(10)) // Wait exactly 10s between attempts
    .build();

queue.add("poll-status", statusPayload, options);
```

---

## Pluggable Custom Backoff Strategies

When default fixed or exponential backoff does not meet specialized requirements (such as decorrelated jitter, fibonacci delays, or error-code-specific backoffs), you can define and register **Custom Backoff Strategies**:

### 1. Configure Job with Custom Strategy Name:
```java
JobOptions options = JobOptions.builder()
    .attempts(5)
    .backoff(BackoffStrategy.custom("jitteredExponential"))
    .build();

queue.add("process-order", orderData, options);
```

### 2. Register Strategy on Worker:
```java
OxmqWorker<OrderData> worker = OxmqWorker.<OrderData>builder()
    .queueName("process-order")
    .redisUri("redis://localhost:6379")
    // Register custom strategy handler: (attemptsMade, throwable, job) -> delayMillis
    .backoffStrategy("jitteredExponential", (attemptsMade, err, job) -> {
        // Dynamic jitter calculation:
        long baseDelay = (long) Math.min(1000 * Math.pow(2, attemptsMade - 1), 60000);
        long jitter = ThreadLocalRandom.current().nextLong(500);
        
        // Return delay in milliseconds (or return -1 to abort retries and fail immediately)
        return baseDelay + jitter;
    })
    .processor(job -> process(job.getData()))
    .build();
```

*Note*: If a custom backoff strategy returns `-1` (or negative delay), OxMQ immediately marks the job as unrecoverable and transitions it to `FAILED` without retrying.

---

## Programmatic Job Discarding (`job.discard()`)

In addition to throwing `UnrecoverableError`, processors can call `job.discard()` at any point before or during failure handling:

```java
OxmqWorker<PaymentPayload> worker = OxmqWorker.<PaymentPayload>builder()
    .queueName("payments")
    .processor(job -> {
        PaymentPayload payment = job.getData();
        if (payment.isCardStolen()) {
            job.discard(); // Aborts all retries; job will immediately transition to FAILED
            throw new IllegalStateException("Card flagged as stolen");
        }
        return charge(payment);
    })
    .build();
```

---

## Bypassing Retries (`UnrecoverableError`)

Some failures should never be retried (e.g. invalid user input, permanent authentication 401/403, missing required fields). Throwing `UnrecoverableError` immediately transitions the job to `FAILED` without exhausting remaining attempts:

```java
import io.oxmq.exception.UnrecoverableError;

@OxmqListener(queue = "documents")
public void parseDocument(Job<DocRequest> job) {
    DocRequest doc = job.getData();

    if (!doc.fileFormat().equals("PDF")) {
        throw new UnrecoverableError("Unsupported format: " + doc.fileFormat() + ". Aborting retries.");
    }

    docProcessor.parse(doc);
}
```

---

## Worker Postponement (`DelayedException`)

When downstream systems signal rate limits or require cool-off, throw `DelayedException`. The job is moved to `delayed` without consuming an attempt or recording failure reasons:

```java
import io.oxmq.exception.DelayedException;

if (downstreamThrottled()) {
    // Postpone for 15 seconds; attemptsMade remains unchanged!
    throw new DelayedException(Duration.ofSeconds(15));
}
```

---

## Replaying Failed Jobs (Dead-Letter Replay)

When an issue is fixed in production (e.g. downstream service restored), re-queue failed jobs back to `WAITING`:

```java
// Re-queue specific failed job with options
queue.retry(failedJobId, RetryOptions.builder()
    .resetAttemptsMade(true)
    .resetAttemptsStarted(true)
    .build());
```

BullMQ's **`reprocessJob-9.lua`** script clears previous errors, checks job priority, and automatically places prioritized jobs back into the priority sorted set (`KEYS[8] prioritized`) so priority ordering is strictly preserved!

---

## Cluster-Wide Bulk Retries (`retryJobs`)

Retry batches of failed or completed jobs across your queue with a single atomic Redis roundtrip:

```java
import io.oxmq.model.RetryJobsOptions;

// Bulk retry up to 500 failed jobs
long retriedCount = queue.retryJobs(RetryJobsOptions.builder()
    .state("failed")
    .count(500)
    .resetAttemptsMade(true)
    .build()
);
```

BullMQ's **`moveJobsToWait-8.lua`** atomically moves the jobs to `wait` and automatically re-attaches parent-child dependencies if any retried jobs belong to DAG workflows.
