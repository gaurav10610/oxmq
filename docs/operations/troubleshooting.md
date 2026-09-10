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

## 3. Redis Connection Pool Starvation

### Symptom:
`JedisException: Could not get a resource from the pool` or `Timeout waiting for idle object`.

### Resolution:
- Increase `pool.maxTotal` in `application.yml` or your `JedisPoolConfig`.
- Rule of thumb: `maxTotal >= (concurrency * numberOfWorkers) + 10`.
- Ensure all custom Redis commands borrowed from the pool are closed in a `try-with-resources` block.

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
