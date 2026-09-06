# 📊 Observability & Telemetry Guide

OxMQ is engineered from the ground up with deep, production-grade observability:
1. **Micrometer Telemetry**: Native timers, counters, and gauges for Prometheus, Grafana, Datadog, and New Relic.
2. **Bull-Board Web UI**: Instant zero-code web dashboard for viewing queues, retrying failed jobs, and reading real-time logs.
3. **QueueEvents Redis Pub/Sub**: Real-time event streaming for WebSocket frontend clients.

---

## 📈 1. Micrometer Metrics Reference

| Metric Name | Type | Tags | Description |
| :--- | :--- | :--- | :--- |
| `oxmq.jobs.enqueued` | Counter | `queue` | Total number of jobs enqueued |
| `oxmq.jobs.completed` | Counter | `queue` | Total number of successfully completed jobs |
| `oxmq.jobs.failed` | Counter | `queue`, `error_type` | Total number of failed jobs by exception type |
| `oxmq.jobs.retried` | Counter | `queue` | Total number of retried attempts |
| `oxmq.jobs.stalled` | Counter | `queue` | Total number of stalled jobs detected and recovered |
| `oxmq.jobs.active` | Gauge | `queue` | Current number of active jobs being processed |
| `oxmq.jobs.waiting` | Gauge | `queue` | Current number of jobs waiting in the queue |
| `oxmq.jobs.delayed` | Gauge | `queue` | Current number of delayed / scheduled jobs |
| `oxmq.job.duration` | Timer | `queue` | Job execution duration (exports `p50`, `p95`, `p99` latency) |
| `oxmq.job.wait_time` | Timer | `queue` | Time spent waiting in queue before execution |

---

## 🖥️ 2. Bull-Board Web UI Setup

Because OxMQ uses BullMQ's standard Redis schema, you can run **Bull-Board** with zero custom code:

```bash
npx @bull-board/cli --redis redis://localhost:6379 --queues notifications,order-invoices,audit-logs
```

Navigate to `http://localhost:3000` in your browser:
* View active, waiting, completed, and failed job counts.
* Inspect JSON payloads and error stack traces.
* Trigger manual retries on dead-letter jobs.
* View step logs published via `job.log(message)`.

---

## 📊 3. Turn-Key Grafana & Prometheus Stack

Our provided `docker-compose.yml` pre-configures Prometheus scraping and provisions the official OxMQ dashboard in Grafana:

```bash
docker compose up -d
```

* **Prometheus**: `http://localhost:9090`
* **Grafana**: `http://localhost:3001` (Credentials: `admin` / `admin`)
* **Pre-Loaded Dashboard**: `OxMQ :: Distributed Job Queue Dashboard` (UID: `oxmq-queue-metrics`)

---

## 📡 4. Real-Time Event Streaming with `QueueEvents`

Listen to job state transitions in real time using Redis Pub/Sub:

```java
import io.oxmq.QueueEvents;
import io.oxmq.event.QueueEventListener;

QueueEvents events = new QueueEvents("order-invoices", "redis://localhost:6379");

events.addListener(new QueueEventListener() {
    @Override
    public void onCompleted(String jobId, String result) {
        System.out.printf("Job %s completed with result: %s%n", jobId, result);
    }

    @Override
    public void onFailed(String jobId, String failedReason) {
        System.err.printf("Job %s failed: %s%n", jobId, failedReason);
    }

    @Override
    public void onProgress(String jobId, int progress) {
        System.out.printf("Job %s progress: %d%%%n", jobId, progress);
    }
});

events.start();
```
