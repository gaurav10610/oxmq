# Rate Limiting & Schedulers

OxMQ provides built-in rate limiters, debouncing, deduplication, and cron schedulers running directly on Redis.

---

## 🚦 Sliding-Window Rate Limiting

Limit job execution rates across distributed worker clusters without relying on distributed locks or complex external coordinators.

```java
OxmqWorker<WebhookPayload> worker = OxmqWorker.<WebhookPayload>builder()
        .queueName("external-webhooks")
        .jedisPool(jedisPool)
        .rateLimit(RateLimit.builder()
                .max(100)                      // Max 100 jobs
                .duration(Duration.ofSeconds(1)) // Per 1-second window
                .build())
        .processor(job -> sendWebhook(job.getData()))
        .build();
```

### Key Grouping (`groupKey`)

You can partition rate limits by tenant, customer, or API key:

```java
JobOptions options = JobOptions.builder()
        .groupKey("tenant_org_456") // Enforce rate limit per tenant
        .build();

queue.add("sync-account", payload, options);
```

---

## 🛑 Debounce & Deduplication

Prevent rapid duplicate submissions of identical tasks:

```java
JobOptions options = JobOptions.builder()
        .deduplicationId("user_order_checkout_789")
        .debounceDuration(Duration.ofSeconds(10)) // Wait for quiet window
        .build();

queue.add("checkout", payload, options);
```

If another request with the same `deduplicationId` arrives within the debounce window, it resets the timer and avoids duplicate processing.

---

## ⏱️ Cron & Recurring Schedulers

OxMQ implements BullMQ's recurring job scheduler factory (`addJobScheduler-11.lua`):

```java
// Schedule a job to run every 15 minutes
queue.upsertJobScheduler(
        "nightly-sync-scheduler",
        JobSchedulerOptions.builder()
                .cron("*/15 * * * *")
                .startDate(Instant.now())
                .build(),
        "sync-ledger",
        new LedgerPayload("all-tenants")
);

// Remove the scheduler when no longer needed
queue.removeJobScheduler("nightly-sync-scheduler");
```
