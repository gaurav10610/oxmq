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

## ⚡ Visual Guide to Core Capabilities

OxMQ handles complex distributed queue patterns with atomic Redis primitives and Java 21 Loom Virtual Threads.

<div style="margin-top: 32px;">

### ⏱️ Delayed & Scheduled Jobs
Schedule jobs with millisecond precision. Atomic Redis sorted sets hold jobs until maturity, promoting them immediately with zero polling lag.

<div style="max-width: 780px; margin: 16px auto 48px; border-radius: 12px; overflow: hidden; border: 1px solid var(--vp-c-divider); box-shadow: 0 8px 30px rgba(0,0,0,0.06);">
  <img src="/assets/oxmq-delayed-jobs.gif" alt="Delayed Job Execution" style="width: 100%; display: block;" loading="lazy">
</div>

### 🔁 Exponential Backoff & Smart Retries
Transient API failures automatically retry with configurable exponential backoff and jitter, preserving dead-lettered jobs in FAILED state for inspection.

<div style="max-width: 780px; margin: 16px auto 48px; border-radius: 12px; overflow: hidden; border: 1px solid var(--vp-c-divider); box-shadow: 0 8px 30px rgba(0,0,0,0.06);">
  <img src="/assets/oxmq-retries-backoff.gif" alt="Exponential Backoff Retries" style="width: 100%; display: block;" loading="lazy">
</div>

### 🚦 Sliding Window Rate Limiting
Safeguard downstream APIs against bursts. Redis token bucket limiters throttle execution across all workers with optional per-tenant grouping.

<div style="max-width: 780px; margin: 16px auto 48px; border-radius: 12px; overflow: hidden; border: 1px solid var(--vp-c-divider); box-shadow: 0 8px 30px rgba(0,0,0,0.06);">
  <img src="/assets/oxmq-rate-limiting.gif" alt="Rate Limiting" style="width: 100%; display: block;" loading="lazy">
</div>

### 🌲 Parent-Child DAG Workflows
Submit dependency trees atomically via FlowProducer. Parallel children execute across worker clusters, automatically aggregating results into the parent task.

<div style="max-width: 780px; margin: 16px auto 48px; border-radius: 12px; overflow: hidden; border: 1px solid var(--vp-c-divider); box-shadow: 0 8px 30px rgba(0,0,0,0.06);">
  <img src="/assets/oxmq-dag-workflow.gif" alt="Parent-Child DAG Workflows" style="width: 100%; display: block;" loading="lazy">
</div>

</div>
