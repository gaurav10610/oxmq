# Pattern: Retries & Backoff Strategies

Transient failures (such as temporary network glitches, rate limit throttles, or database deadlocks) are normal in distributed environments. OxMQ provides configurable retry policies with automated backoff schedules.

---

## 🔁 Exponential Backoff (Recommended)

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

## ⏱️ Fixed Backoff

If your workload requires a constant retry interval:

```java
JobOptions options = JobOptions.builder()
    .attempts(3)
    .fixedBackoff(Duration.ofSeconds(10)) // Wait exactly 10s between attempts
    .build();

queue.add("poll-status", statusPayload, options);
```

---

## 🚫 Bypassing Retries (`UnrecoverableError`)

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

## 🔁 Replaying Failed Jobs (Dead-Letter Replay)

When an issue is fixed in production (e.g. downstream service restored), re-queue failed jobs back to `WAITING`:

```java
// Re-queue specific failed job
queue.retry(failedJobId);
```
BullMQ's `reprocessJob-7.lua` script removes the job from `failed`, clears the previous error message, and pushes it back onto `wait`.
