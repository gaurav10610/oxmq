# 🛡️ Production Deployment & Hardening Checklist

Before deploying **OxMQ** into high-traffic production environments (Kubernetes, ECS, Bare-Metal), follow this battle-tested operational checklist.

---

## 🗄️ 1. Redis Server Configuration

### 🚨 Critical: Set Eviction Policy to `noeviction`
By default, Redis may evict keys when reaching `maxmemory`. In a job queue, key eviction causes lost jobs and corrupted state!
```ini
# redis.conf
maxmemory 4gb
maxmemory-policy noeviction
```

### 💾 Persistence (AOF & RDB)
To prevent data loss across Redis restarts:
```ini
# Enable Append-Only File with 1-second sync
appendonly yes
appendfsync everysec

# Periodic snapshots
save 900 1
save 300 10
save 60 10000
```

---

## ⚙️ 2. Worker Tuning & Virtual Threads

### Concurrency
* For **I/O-bound workloads** (HTTP APIs, database calls, external webhooks, email delivery), set concurrency to **`50` to `500+`** Virtual Threads per instance.
* For **CPU-intensive workloads** (image encoding, heavy compression), limit concurrency to $N_{\text{CPUs}} \times 2$.

### Lock Duration (`lockDurationMs`)
* Set `lockDurationMs` to **$2\times$ to $3\times$** your expected maximum job runtime (default: `30,000ms`).
* OxMQ's built-in `LockExtender` automatically renews active job locks periodically while the job is still executing.

---

## 🚢 3. Kubernetes & Graceful Shutdown (`SIGTERM`)

When Kubernetes scales down pods or rolls out new deployments, workers must drain active jobs without aborting in-flight work:

```yaml
spec:
  containers:
    - name: oxmq-microservice
      image: myorg/oxmq-service:1.0.0
      # Allow Kubernetes enough time for workers to drain tasks
      terminationGracePeriodSeconds: 30
      env:
        - name: OXMQ_REDIS_URI
          value: "redis://redis-master.internal:6379"
```

In your Java application, `OxmqWorker.stop()` automatically drains running tasks up to `drainTimeoutMs` (default: 10 seconds) before releasing locks.

---

## 🔍 4. Observability & Alerting Thresholds

Set up the following Prometheus alerts in your monitoring stack:

1. **High Queue Backlog Alert**:
   ```promql
   oxmq_jobs_waiting_value{queue="notifications"} > 10000 for 5m
   ```
2. **High Error Rate Alert**:
   ```promql
   sum(rate(oxmq_jobs_failed_total[5m])) / sum(rate(oxmq_jobs_completed_total[5m])) > 0.05
   ```
3. **Stalled Jobs Alert**:
   ```promql
   rate(oxmq_jobs_stalled_total[5m]) > 0
   ```
