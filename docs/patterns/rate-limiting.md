# Pattern: Rate Limiting & Group Keys

Rate limiting protects downstream services (such as third-party APIs with strict rate quotas like Stripe, Twilio, or OpenAI) from being overwhelmed by bursty queue traffic.

OxMQ implements BullMQ's **sliding window token bucket rate limiter** directly in Redis.

---

## 🚦 Global Queue Rate Limiting

Enforce a maximum number of processed jobs across an entire worker cluster within a sliding time window:

::: code-group

```java [Pure Java]
import io.oxmq.OxmqWorker;
import io.oxmq.model.RateLimit;
import java.time.Duration;

OxmqWorker<WebhookPayload> worker = OxmqWorker.<WebhookPayload>builder()
        .queueName("stripe-webhooks")
        .jedisPool(jedisPool)
        .rateLimit(RateLimit.builder()
                .max(50)                         // Maximum 50 executions
                .duration(Duration.ofSeconds(1)) // Within any 1-second rolling window
                .build())
        .processor(job -> sendStripeRequest(job.getData()))
        .build();
```

```java [Spring Boot]
@Component
public class StripeWebhookListener {

    @OxmqListener(
        queue = "stripe-webhooks",
        concurrency = 20,
        rateLimitMax = 50,              // Max 50 jobs
        rateLimitDurationMs = 1000      // Per 1,000 ms (1 second)
    )
    public void processWebhook(Job<WebhookPayload> job) {
        sendStripeRequest(job.getData());
    }
}
```

:::

---

## 🏢 Multi-Tenant Rate Limiting (`groupKey`)

In SaaS applications, one noisy tenant should not exhaust the API rate quota for other tenants. 

OxMQ supports partitioning rate limits per tenant using `groupKey` in `JobOptions`:

```java
// Job for Tenant A
queue.add(
    "send-notification",
    payloadA,
    JobOptions.builder()
        .groupKey("tenant_acme_corp") // Partitioned rate limit bucket
        .build()
);

// Job for Tenant B
queue.add(
    "send-notification",
    payloadB,
    JobOptions.builder()
        .groupKey("tenant_globex_corp") // Independent rate limit bucket
        .build()
);
```

### How It Works Under the Hood:
1. Each `groupKey` maintains an independent Redis sorted set: `bull:<queue>:limiter:<groupKey>`.
2. As jobs are claimed, timestamps are added to the set.
3. If the group limit is reached, BullMQ's `moveToActive-11.lua` leaves the job in `WAITING` until the sliding window expires, while continuing to process jobs for other tenants!
