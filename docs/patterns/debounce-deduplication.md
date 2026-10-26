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

### Option C: `keepLastIfActive` Deduplication Buffering (BullMQ v6 Parity)

In standard deduplication, if a job with the same deduplication ID is currently running (`ACTIVE`), any new incoming job is rejected/dropped.

With **`keepLastIfActive`**, OxMQ buffers the latest submitted payload in Redis key `dn:<deduplicationId>` while the current job is active. When the currently executing job completes or fails, BullMQ's `moveToFinished-14.lua` automatically re-enqueues the buffered job with the latest payload!

```java
import io.oxmq.model.DeduplicationOptions;

queue.add(
    "generate-report",
    reportPayload,
    JobOptions.builder()
        .deduplication(DeduplicationOptions.builder()
            .id("user_report_101")
            .keepLastIfActive(true)  // Buffer latest update while running
            .ttlMs(60_000L)          // 60-second dedup window
            .build())
        .build()
);
```

This pattern is ideal for cache warming, invoice recalculations, or document synchronization where incoming updates during processing should trigger one final execution with the latest state.

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

## 🔍 Querying & Early Releasing Deduplication Keys

### 1. Query Active Job ID by Deduplication Key
You can query the active job associated with any deduplication ID in $O(1)$ time complexity without scanning queue lists:

```java
// Query via Queue
String activeJobId = queue.getDeduplicationJobId("catalog_reindex");
if (activeJobId != null) {
    System.out.println("Job " + activeJobId + " currently holds deduplication key");
}

// Or via Spring Boot OxmqTemplate
String jobId = oxmqTemplate.getDeduplicationJobId("search-queue", "catalog_reindex");
```

### 2. Early Release of Deduplication Keys
If a job completes or is administratively cancelled and you immediately want to allow new submissions with the same key before the TTL expires:

```java
// Via Queue
queue.removeDeduplicationKey("catalog_reindex");

// Via Spring Boot OxmqTemplate
oxmqTemplate.removeDeduplicationKey("search-queue", "catalog_reindex");
```
BullMQ's `removeDeduplicationKey-1.lua` clears the Redis deduplication lock.
