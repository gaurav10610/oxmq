# Production Troubleshooting Manual

This operational guide provides actionable solutions for diagnosing and resolving common production issues when running OxMQ.

---

## 1. Stalled Jobs Diagnosis

### Symptom:
A job transitions to `ACTIVE`, executes for a while, and is then moved back to `WAITING` or marked `FAILED` with a stalled error.

### Root Causes:
1. **Worker Crashed**: The JVM terminated abnormally (OOM killer, server power loss, SIGKILL), causing the worker lock to expire. The stalled sentinel safely recovered the job.
2. **Event Loop Starvation / Heavy CPU Work**: If your processor executes heavy, uninterruptible CPU computations on carrier threads without yielding, the background heartbeat timer may be delayed.
3. **Network Partition**: The worker lost connection to Redis for longer than the `lockDuration` (default: 30 seconds).

### Resolution:
- If tasks legitimately take longer to complete (e.g. 5+ minutes), increase `lockDuration`:
  ```java
  worker.lockDuration(Duration.ofMinutes(5));
  ```
- Ensure background lock heartbeat timers are not starved.
- Verify worker memory limits in Kubernetes/Docker to avoid OOM kills.

---

## 2. Carrier Thread Pinning in Java 21

### Symptom:
Virtual threads are not unmounting during blocking I/O, leading to carrier thread exhaustion and reduced throughput.

### Root Cause:
In Java 21, virtual threads cannot unmount from carrier threads if blocking I/O occurs inside a `synchronized` block or native method call (known as **pinning**).

### Resolution:
- Replace `synchronized (lock)` with `java.util.concurrent.locks.ReentrantLock`:
  ```java
  // ❌ Avoid: Pins carrier thread during blocking I/O
  synchronized(mutex) {
      httpClient.send(...);
  }

  // ✅ Recommended: Unmounts smoothly on blocking I/O
  private final ReentrantLock lock = new ReentrantLock();
  lock.lock();
  try {
      httpClient.send(...);
  } finally {
      lock.unlock();
  }
  ```
- Run JVM with `-Djdk.tracePinnedThreads=short` to log any pinning events during staging testing.

---

## 3. Redis Connection & Timeout Issues

### Symptom:
`RedisConnectionException: Unable to connect to Redis` or `RedisCommandTimeoutException: Command timed out after 5000ms`.

### Resolution:
- Check Redis server liveness and network latency: `redis-cli ping`.
- For high-concurrency workloads, verify the Redis `timeout` configuration in `redis.conf` (ensure it is `0` or sufficiently high for persistent connection pooling).
- Ensure client connections are cleanly closed upon application shutdown via `@PreDestroy` or Spring Boot context closure.

---

## 4. Redis Memory Eviction Policies

### Critical Requirement:
Never configure Redis with `allkeys-lru` or `allkeys-random` when hosting message queues! 

If Redis runs low on memory, LRU eviction will delete queue lists (`bull:<queue>:wait`) and metadata hashes, corrupting queue state.

### Recommended Configuration:
```text
# redis.conf
maxmemory 4gb
maxmemory-policy noeviction
```
With `noeviction`, Redis will reject new writes with an error rather than silently deleting existing queue messages. Use OxMQ's `queue.clean(...)` or `removeOnComplete` options to prune completed jobs.

---

## 5. Sudden Redis Crash & Automatic Reconnection

### Symptom:
Redis server crashes abruptly due to hardware failure, kernel OOM termination, or cloud provider maintenance reboot. Workers throw connection dropped exceptions.

### Root Cause:
TCP connection terminated unexpectedly while workers were actively polling or executing jobs.

### Resolution & Behavior:
- **Lettuce Auto-Reconnect**: OxMQ utilizes Lettuce, which enters automatic reconnection mode upon socket failure. Command buffers hold pending non-blocking calls, and polling loops sleep and retry with exponential backoff rather than crashing the JVM.
- **In-Flight Job Recovery**: When Redis returns, jobs whose distributed locks (`bull:<queue>:<jobId>:lock`) expired are recovered by `StalledJobSentinel` and moved back to `WAITING` without human intervention.
- **Persistence Verification**: Ensure Redis AOF (`appendonly yes` with `appendfsync everysec`) is active so state survives node restarts.

---

## 6. Sudden Pod Kills & Kubernetes Preemption

### Symptom:
Kubernetes autoscaler or node draining terminates worker pods with SIGTERM or SIGKILL midway through heavy batch ingestion or directory crawls, resulting in duplicate processing or incomplete tasks.

### Resolution:
1. **Adopt `CheckpointStatePlugin`**: Persist delta tokens and pagination cursors in Redis (`oxmq:checkpoint:<namespace>`). When a pod is replaced, the new worker reads the committed checkpoint and resumes without re-processing already inspected items.
2. **Configure Kubernetes Graceful Termination**:
   - Set `terminationGracePeriodSeconds: 45` in the Deployment spec.
   - Add a `preStop` hook: `preStop: exec: command: ["sh", "-c", "sleep 10"]` to deregister the pod from ingress routing before SIGTERM is delivered.
   - Enable `server.shutdown=graceful` in `application.yml` with a 30s drain timeout.

---

## 7. Cloud Provider 429 & 503 Throttling Storms

### Symptom:
Workers calling third-party SaaS APIs (Microsoft Graph, Salesforce, Stripe, OpenAI) encounter HTTP 429 Too Many Requests or HTTP 503 Service Unavailable, leading to cascading job failures and API account bans.

### Resolution:
1. **Deploy `AdaptiveRateLimiterPlugin`**: Automatically halves request capacity upon encountering 429/503 errors and locks against oscillation for 30–60 seconds, allowing downstream cloud gateways to recover.
2. **Deploy `RateLimitSentinelPlugin`**: Sets a cluster-wide Redis cooldown key matching the `Retry-After` header. All cluster pods immediately defer upcoming jobs for that tenant without executing outbound HTTP requests.
3. **Deploy `PriorityQuotaGovernorPlugin`**: Forces low-priority background jobs to yield when tenant token headroom drops below 25%, strictly reserving quota for high-priority real-time events.

---

## 8. SRE Production Disaster Recovery Checklist

- [x] Redis configured with `maxmemory-policy noeviction` and `appendonly yes`.
- [x] Lettuce connection pooling with automatic reconnection enabled.
- [x] Kubernetes deployments configured with `preStop: sleep 10` and `terminationGracePeriodSeconds: 45`.
- [x] Liveness (`/actuator/health/liveness`) and readiness (`/actuator/health/readiness`) probes configured.
- [x] `CheckpointStatePlugin` registered on all multi-page or multi-item crawling workers.
- [x] `AdaptiveRateLimiterPlugin` and `RateLimitSentinelPlugin` active on all third-party API queues.
- [x] Prometheus scraping active and Grafana dashboards (`oxmq-grafana-dashboard.json`, `oxmq-system-metrics.json`) imported.
