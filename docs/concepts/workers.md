# Workers

A **Worker** is the consumer engine in OxMQ responsible for claiming waiting jobs from Redis, executing user code concurrently on **Java 21 Virtual Threads**, extending locks via heartbeats, reporting completion or failure, and recovering stalled jobs.

---

## 🏗️ Worker Architecture

```text
+-------------------------------------------------------------------------+
|                               OxMQ Worker                               |
|                                                                         |
|  +------------------------+      +-----------------------------------+  |
|  |     Polling Loop       | ---> | Virtual Thread Dispatcher         |  |
|  | (moveToActive-11.lua)  |      | (Executors.newVirtualThread...)   |  |
|  +------------------------+      +-----------------+-----------------+  |
|                                                    |                    |
|                                                    v                    |
|                                  +-----------------------------------+  |
|                                  | User Processor Lambda             |  |
|                                  | job -> process(job.getData())     |  |
|                                  +-----------------+-----------------+  |
|                                                    |                    |
|  +------------------------+                        v                    |
|  |  Heartbeat Lock Timer  | <--- [Active Task Execution]                |
|  |   (extendLock-2.lua)   |                        |                    |
|  +------------------------+                        v                    |
|                                  +-----------------------------------+  |
|  +------------------------+      | Completion Reporter               |  |
|  |  Stalled Job Sentinel  |      | (moveToFinished-14.lua)           |  |
|  | (moveStalledJobsToWait)|      +-----------------------------------+  |
|  +------------------------+                                             |
+-------------------------------------------------------------------------+
```

---

## 🧵 Concurrency & Virtual Threads

OxMQ allows configuring high concurrency without the risk of OS thread pool exhaustion:

```java
OxmqWorker<OrderPayload> worker = OxmqWorker.<OrderPayload>builder()
        .queueName("orders")
        .jedisPool(jedisPool)
        .payloadClass(OrderPayload.class)
        .concurrency(100)           // Up to 100 concurrent jobs executed simultaneously
        .useVirtualThreads(true)    // Dispatched to lightweight Java 21 Loom threads
        .processor(job -> handleOrder(job.getData()))
        .build();

worker.start();
```

### Why Virtual Threads Excel for Workers
When your job processor performs blocking operations (calling a third-party payment gateway, executing SQL queries via JDBC, sending emails, or waiting on microservices), the Virtual Thread **unmounts from its carrier OS thread**. 

The carrier OS thread is immediately free to process other jobs, allowing a single OxMQ node to run thousands of concurrent blocking tasks with negligible memory overhead (~1 KB heap per virtual thread vs 1 MB native stack per platform thread).

---

## 🔒 Heartbeat Lock Watchdog & Auto-Extension

When a worker claims a job from Redis, it acquires an exclusive distributed lock:
- **Default Lock TTL**: 30,000 ms (30 seconds).
- **Auto-Extension Heartbeat**: While the job is running, OxMQ runs a background timer that executes BullMQ's `extendLock-2.lua` every 15 seconds (`lockDuration / 2`).
- **No Early Expiration**: Long-running jobs (e.g. video processing or large data imports taking 10+ minutes) will never have their locks expire as long as the worker process remains alive and healthy.

---

## 🛡️ Stalled Job Recovery Sentinel

If a worker node crashes abruptly (e.g. `SIGKILL`, server crash, hardware power cut, OutOfMemory):
1. The heartbeat timer stops extending the lock.
2. The lock expires in Redis.
3. Other active workers run BullMQ's `moveStalledJobsToWait-9.lua` during their background stalled check interval (default: every 30 seconds).
4. The orphaned job is automatically recovered, moved back to `bull:<queue>:wait`, and claimed by another healthy worker.

### Limiting Stalled Retries (`maxStalledCount`)

If a job causes a JVM crash (e.g. fatal segmentation fault or native library crash), you can limit how many times it can be recovered:

```java
OxmqWorker.<Payload>builder()
        .maxStalledCount(1) // Fail job if it stalls more than once
        ...
```

---

## 🛑 Worker Lifecycle & Graceful Shutdown

Always shut down workers cleanly so in-flight tasks finish without stalling:

```java
// Register JVM shutdown hook
Runtime.getRuntime().addShutdownHook(new Thread(() -> {
    System.out.println("Shutting down worker gracefully...");
    worker.close(); // Halts new job acquisition and waits for active jobs to complete
    System.out.println("Worker shut down cleanly.");
}));
```

### Pausing & Resuming Worker Locally

```java
// Pause this specific worker instance from taking new jobs
worker.pause();

// Resume processing
worker.resume();
```
