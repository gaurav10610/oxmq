---
layout: home

hero:
  name: "OxMQ"
  text: "Distributed Queue Engine for Java 21 Loom"
  tagline: "100% wire and functional parity with BullMQ v5. Powered by Project Loom Virtual Threads."
  image:
    src: /assets/oxmq-logo.png
    alt: OxMQ Logo
  actions:
    - theme: brand
      text: Get Started
      link: /guide/what-is-oxmq
    - theme: alt
      text: Spring Boot Guide
      link: /guide/spring-boot
    - theme: alt
      text: View on GitHub
      link: https://github.com/gaurav10610/oxmq

features:
  - icon: 🚀
    title: Project Loom Virtual Threads
    details: Replace heavy OS threads with lightweight Java 21 Virtual Threads. Handle thousands of concurrent blocking I/O jobs without pool starvation.
  - icon: ⚡
    title: 100% BullMQ v5 Parity
    details: Direct execution of 49 official BullMQ Lua scripts for atomic Redis state transitions, rate limiting, and parent-child workflows.
  - icon: 🍃
    title: Spring Boot 3+ Native Starter
    details: Declarative @OxmqListener annotations, auto-configured connection pools, automated lifecycle management, and Actuator metrics.
  - icon: 🌲
    title: Complex Flow Trees (DAGs)
    details: Orchestrate parent-child dependency trees with FlowProducer, automated child failure propagation, and aggregated result gathering.
  - icon: 🎛️
    title: Bull-Board UI Ready
    details: Instant visual observability into queue depths, active jobs, worker locks, and execution logs using standard Bull-Board.
  - icon: ⏱️
    title: Schedulers & Rate Limiters
    details: Cron recurring jobs, sliding-window token bucket limiters with key grouping, and customizable debounce intervals.
---

## Quick Example

::: code-group

```java [Standalone Producer]
JedisPool pool = new JedisPool("localhost", 6379);
Queue<OrderPayload> queue = new OxmqQueue<>("orders", pool);

// Enqueue job with automatic retry & backoff
Job<OrderPayload> job = queue.add("process-payment", new OrderPayload("ord_101", 99.00));
System.out.println("Enqueued job ID: " + job.getId());
```

```java [Spring Boot Worker]
@Component
public class PaymentWorker {

    @OxmqListener(queue = "orders", concurrency = 50)
    public void handlePayment(Job<OrderPayload> job) {
        OrderPayload order = job.getData();
        // Virtual threads safely handle blocking HTTP or DB calls
        paymentGateway.charge(order.amount());
    }
}
```

```yaml [application.yml]
oxmq:
  redis:
    host: localhost
    port: 6379
  worker:
    virtual-threads: true
    default-concurrency: 50
    stalled-interval-ms: 30000
```

:::

---

## ⚡ Core Scenarios in Action

Explore how OxMQ handles the real-world operational challenges of distributed queues.

<br/>

### ⏱️ 1. Schedule Jobs for Later
Process jobs at an exact future timestamp or after a relative delay. Perfect for payment verification windows, reminder emails, or delayed webhook retries.
- **Millisecond Precision**: Backed by atomic Redis sorted sets (`bull:<queue>:delayed`).
- **Zero Polling Lag**: Automatically promoted to `WAITING` the exact millisecond maturity is reached.
- **Survives Restarts**: State is persisted in Redis with zero relational database table locks.

```java
// Schedule job to execute exactly 15 minutes from now
queue.add(
    "send-reminder",
    new ReminderPayload("user_42"),
    JobOptions.builder().delay(Duration.ofMinutes(15)).build()
);
```

<div style="border-radius: 12px; overflow: hidden; border: 1px solid var(--vp-c-divider); box-shadow: 0 4px 20px rgba(0,0,0,0.06); margin-top: 16px;">
  <img src="/assets/oxmq-delayed-jobs.gif" alt="Delayed Job Execution Animation" style="width: 100%; display: block;">
</div>

<br/>
<hr style="border: 0; border-top: 1px solid var(--vp-c-divider); margin: 32px 0;"/>
<br/>

### 🔁 2. Failures Are Temporary
When third-party APIs throttle connections or return transient 503 errors, OxMQ automatically reschedules jobs with exponential backoff.
- **Exponential Backoff**: Configurable multiplier with maximum backoff caps to prevent storming.
- **Fatal Error Bypass**: Throw `UnrecoverableError` to fail immediately and skip retries on permanent failures.
- **Zero Loss Dead-Lettering**: Exhausted jobs are preserved in `FAILED` and can be re-queued with `queue.retry(jobId)`.

```java
// Configure 5 retry attempts with exponential backoff capped at 5 minutes
JobOptions options = JobOptions.builder()
    .attempts(5)
    .exponentialBackoff(Duration.ofSeconds(2), Duration.ofMinutes(5))
    .build();

queue.add("capture-charge", payment, options);
```

<div style="border-radius: 12px; overflow: hidden; border: 1px solid var(--vp-c-divider); box-shadow: 0 4px 20px rgba(0,0,0,0.06); margin-top: 16px;">
  <img src="/assets/oxmq-retries-backoff.gif" alt="Exponential Backoff Retry Animation" style="width: 100%; display: block;">
</div>

<br/>
<hr style="border: 0; border-top: 1px solid var(--vp-c-divider); margin: 32px 0;"/>
<br/>

### 🚦 3. Protect Your Downstream APIs
Safeguard external services (Stripe, Twilio, OpenAI) by enforcing rate limits directly in Redis across your entire worker cluster.
- **Sliding Window Token Bucket**: Redis Lua script enforces smooth rate quotas without distributed lock overhead.
- **Multi-Tenant Grouping (`groupKey`)**: Partition rate limits per tenant so one noisy customer never throttles others.
- **Debounce & Deduplication**: Built-in trailing-edge debounce windows ignore rapid duplicate triggers.

```java
@OxmqListener(
    queue = "stripe-webhooks",
    concurrency = 20,
    rateLimitMax = 50,          // Max 50 requests
    rateLimitDurationMs = 1000  // Per 1-second rolling window
)
public void handleWebhook(Job<WebhookPayload> job) {
    stripeClient.process(job.getData());
}
```

<div style="border-radius: 12px; overflow: hidden; border: 1px solid var(--vp-c-divider); box-shadow: 0 4px 20px rgba(0,0,0,0.06); margin-top: 16px;">
  <img src="/assets/oxmq-rate-limiting.gif" alt="Sliding Window Rate Limiter Animation" style="width: 100%; display: block;">
</div>

<br/>
<hr style="border: 0; border-top: 1px solid var(--vp-c-divider); margin: 32px 0;"/>
<br/>

### 🌲 4. Complex Workflows (Parent-Child DAGs)
Orchestrate multi-step task trees where parent tasks wait for parallel children to finish before executing.
- **Atomic DAG Enqueue**: The entire workflow tree is submitted in one atomic Redis call via `FlowProducer`.
- **Parallel Child Processing**: Children execute in parallel across independent queues and worker clusters.
- **Automated Result Aggregation**: Child return values are automatically stored in the parent's processed hash for consumption.

```java
FlowJob<ReportSummary> parent = FlowJob.<ReportSummary>builder()
    .queueName("reports")
    .name("compile-report")
    .children(List.of(fetchAnalyticsChild, auditFinanceChild))
    .build();

flowProducer.add(parent);
```

<div style="border-radius: 12px; overflow: hidden; border: 1px solid var(--vp-c-divider); box-shadow: 0 4px 20px rgba(0,0,0,0.06); margin-top: 16px;">
  <img src="/assets/oxmq-dag-workflow.gif" alt="FlowProducer DAG Workflow Animation" style="width: 100%; display: block;">
</div>
