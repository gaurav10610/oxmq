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
        .redisUri("redis://localhost:6379")
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

---

## 🚀 Enterprise Scaling: Beyond Basic Rate Limiting

While standard `groupKey` protects individual queues with static thresholds, enterprise multi-tenant architectures often require:
- **Dynamic Tier Limits**: Scaling burst capacity based on active subscription tiers (e.g. 50 req/s for SMB vs 500 req/s for Enterprise).
- **Cluster-Wide Circuit Breaking (HTTP 429 Sentinel)**: Automatically deferring upcoming jobs when external SaaS APIs trigger throttling, without burning retry attempts.
- **Priority Headroom Governance**: Forcing low-priority historical sweeps to yield quota when a tenant approaches quota exhaustion, strictly reserving headroom for real-time operations.

For these capabilities, see the [Plugins & Interceptors Guide](/guide/plugins) for:
- [`GroupRateLimiterPlugin`](/guide/plugins#1-groupratelimiterplugin-multi-tenant-token-bucket)
- [`RateLimitSentinelPlugin`](/guide/plugins#2-ratelimitsentinelplugin-external-429-cooldown-sentinel)
- [`PriorityQuotaGovernorPlugin`](/guide/plugins#3-priorityquotagovernorplugin-headroom-based-yielding)

