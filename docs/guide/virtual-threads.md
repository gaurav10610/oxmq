# Virtual Threads & Concurrency (Project Loom)

One of OxMQ's core architectural advantages is its native foundation on **Java 21 Project Loom Virtual Threads**.

---

## 🧵 OS Platform Threads vs. Virtual Threads

Traditional Java message queues (and older Redis queue wrappers) rely on standard OS platform threads managed by a `ThreadPoolExecutor`:

```
Traditional OS Thread Pool:
[Job 1] ---> OS Thread 1 (Blocks on REST API) -----> [Carrier Thread Held: 1 MB RAM]
[Job 2] ---> OS Thread 2 (Blocks on DB Query) ------> [Carrier Thread Held: 1 MB RAM]
...
[Job 200] -> Thread Pool Exhausted! (Queue backlog accumulates)
```

In contrast, **OxMQ** dispatches each claimed job to an unpinned Java 21 Virtual Thread:

```
OxMQ Virtual Thread Architecture:
[Job 1] ---> Virtual Thread 1 (Blocks on REST API) --> [Unmounts carrier thread: ~1 KB RAM]
[Job 2] ---> Virtual Thread 2 (Blocks on DB Query)  --> [Unmounts carrier thread: ~1 KB RAM]
...
[Job 10,000] -> Runs concurrently across small pool of carrier OS cores!
```

---

## 🚀 Key Concurrency Advantages

### 1. Massive I/O Concurrency
Virtual threads unmount from the underlying operating system carrier thread whenever they enter a blocking call (e.g. `Thread.sleep()`, HTTP client calls, JDBC queries, socket I/O). The carrier thread is immediately freed to execute other waiting jobs.

### 2. Microscopic Memory Footprint
A platform OS thread allocates 1 MB of stack memory by default. Running 1,000 platform threads requires ~1 GB of RAM just for thread stacks. In contrast, an idle virtual thread occupies roughly 1 KB of heap memory, enabling tens of thousands of concurrent tasks without memory pressure.

### 3. Elimination of Thread Pool Sizing Headaches
With traditional thread pools, choosing the right pool size is notoriously difficult: too small causes worker starvation; too large causes high context-switching overhead and OutOfMemory errors. With Virtual Threads in OxMQ, developers simply configure the desired queue concurrency limit (e.g. `concurrency = 100` or `1000`) without risking thread pool exhaustion.

---

## ☕ Java 17 LTS Compatibility & Graceful Fallback

OxMQ is compiled with `--release 17` to support both **Java 17 LTS** and **Java 21+ LTS**.

### Transparent Runtime Dispatching (`DispatcherExecutorFactory`)
When an `OxmqWorker` or `@OxmqListener` initializes:
1. **JVM Detection**: `DispatcherExecutorFactory` checks the runtime JVM version using `Runtime.version().feature()`.
2. **On Java 21+**: Spawns an unpinned `VirtualThreadPerTaskExecutor` (`Thread.ofVirtual()`), enabling massive concurrency with ~2 KB per task.
3. **On Java 17**: Transparently falls back to a tuned platform daemon thread pool sized to the worker's configured concurrency (`Executors.newFixedThreadPool(concurrency)`).

```java
// Under the hood in DispatcherExecutorFactory:
if (isVirtualThreadSupported() && useVirtualThreads) {
    return (ExecutorService) newVirtualThreadPerTaskExecutorMethod.invoke(null);
} else {
    log.info("Falling back to platform threads for worker '{}'", threadNamePrefix);
    return Executors.newFixedThreadPool(concurrency, platformThreadFactory);
}
```

This guarantees **zero code changes** when moving an application from Java 17 to Java 21 in production!

---

## 🛡️ Best Practices with Loom & OxMQ

1. **Size Downstream Connection Pools**: While Virtual Threads can handle thousands of concurrent operations, downstream systems (such as relational databases like PostgreSQL or MySQL) typically have connection limits. Ensure your HikariCP database pool or HTTP client limits are configured appropriately.
2. **Avoid Pinning**: Ensure your business logic does not execute blocking I/O inside `synchronized` blocks. Use `java.util.concurrent.locks.ReentrantLock` instead.
3. **Keep Redis Connections Adequate**: OxMQ workers acquire Redis connections to poll, extend locks, and report completion. Ensure connection pool limits are sized proportionally to active queues and workers.
