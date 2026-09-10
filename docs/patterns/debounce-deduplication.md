# Pattern: Debounce & Deduplication

In modern web applications, rapid repeated user actions (e.g. double-clicking a payment button, fast search keystrokes, or multiple webhook triggers) can generate duplicate tasks. 

OxMQ provides both **idempotency deduplication** and **debounce windows**.

---

## 🔒 Deterministic Job Deduplication

To guarantee that a job is only enqueued once within a given time frame or for a specific transaction ID:

### Option A: Deterministic `jobId`
If you provide an explicit `jobId`, Redis will refuse to enqueue a duplicate job with the same ID if it is already in `waiting`, `active`, or `delayed`:

```java
// If user clicks "Place Order" 3 times rapidly, only the first call creates a job
queue.add(
    "checkout",
    orderPayload,
    JobOptions.builder()
        .jobId("order_tx_" + orderPayload.transactionId())
        .build()
);
```

### Option B: Dedicated `deduplicationId`
Use a dedicated deduplication ID that does not affect the job's primary ID:

```java
queue.add(
    "sync-user",
    userPayload,
    JobOptions.builder()
        .deduplicationId("user_sync_" + userPayload.userId())
        .build()
);
```

---

## ⏳ Debounce Window (Trailing Edge)

Debouncing waits for a "quiet period" before executing. If another identical request arrives before the debounce timer finishes, the timer resets:

```java
// Wait for 5 seconds of inactivity before compiling search indices
queue.add(
    "reindex-search",
    searchPayload,
    JobOptions.builder()
        .deduplicationId("catalog_reindex")
        .debounceDuration(Duration.ofSeconds(5))
        .build()
);
```

### Real-World Use Case:
- User types search keywords into an auto-complete search box.
- With every keystroke, a `debounceDuration` of 500 ms is applied.
- The heavy indexing task only executes after the user stops typing for at least 500 ms.

---

## 🔓 Early Release of Deduplication Keys

If a job completes and you immediately want to allow new submissions with the same key:

```java
queue.removeDeduplicationKey("catalog_reindex");
```
BullMQ's `removeDeduplicationKey-1.lua` clears the Redis deduplication lock.
