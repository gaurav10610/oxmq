# ⏱️ Sliding-Window Rate Limiting & Throttling

When integrating with third-party APIs (such as OpenAI, Stripe, Shopify, or Twilio), background workers can easily overwhelm upstream rate limits (e.g., HTTP `429 Too Many Requests`), causing dropped requests or temporary account bans.

**OxMQ** includes a built-in, distributed **Sliding-Window Token-Bucket Rate Limiter** powered by atomic Redis Lua scripts (`rateLimit.lua`).

---

## 🎯 How It Works

1. Every queue can have a configured rate limit (e.g. `max: 50, duration: Duration.ofSeconds(1)`).
2. Before popping a job from `wait` to `active`, the worker attempts to acquire a token from Redis.
3. The Lua script evaluates the timestamp sliding window:
   - If tokens are available, the token is recorded and the job executes immediately.
   - If the quota is exceeded, `tryAcquire()` returns `false`, causing the worker to back off until the sliding window clears.
4. **No jobs are dropped**: Pending jobs remain safely queued in Redis until tokens replenish.

---

## 🛠️ Configuration Examples

### 1. Pure Java Configuration

```java
import io.oxmq.OxmqWorker;
import java.time.Duration;

public class OpenAiWorker {
    public static void main(String[] args) {
        OxmqWorker<PromptRequest> worker = OxmqWorker.<PromptRequest>builder()
                .queueName("openai-prompts")
                .redisUri("redis://localhost:6379")
                .concurrency(20)
                // Limit to 5 jobs per 1,000 milliseconds (5 req/sec)
                .rateLimit(5, Duration.ofSeconds(1))
                .processor(job -> {
                    PromptRequest prompt = job.getData();
                    job.log("Calling OpenAI API for model: " + prompt.model());
                    // Execute HTTP call to OpenAI
                    return "LLM_RESPONSE";
                })
                .build();

        worker.start();
    }
}
```

---

### 2. Spring Boot 3 Declarative Configuration

In Spring Boot, configure rate limits directly via `@OxmqListener`:

```java
@Component
public class StripeWebhookDispatcher {

    // Throttle to 100 requests per 60 seconds (Stripe API quota)
    @OxmqListener(
        queue = "stripe-charges",
        concurrency = 50,
        rateLimitMax = 100,
        rateLimitDurationMs = 60000
    )
    public String dispatchStripeCharge(Job<ChargePayload> job) {
        ChargePayload charge = job.getData();
        job.log("Processing Stripe payment: " + charge.chargeId());
        return "SUCCESS";
    }
}
```

---

## 📊 Key Benefits of OxMQ Rate Limiting

* **Cluster-Wide Coordination**: If you run 10 JVM worker instances across Kubernetes pods, all 10 instances share the same Redis token bucket and honor the global limit together.
* **Zero Thread Blocking**: Poller threads briefly sleep and yield without blocking OS carrier threads.
* **Sub-Millisecond Check**: The Lua script evaluates tokens in $< 0.5\text{ms}$.
