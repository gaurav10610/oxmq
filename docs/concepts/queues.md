# Queues

A **Queue** in OxMQ is the primary interface for creating, managing, and inspecting distributed jobs. Behind the scenes, each queue is backed by standard Redis lists, sorted sets, hashes, and streams managed by official BullMQ Lua scripts.

---

## 🏗️ Initializing a Queue

::: code-group

```java [Pure Java]
import io.oxmq.OxmqQueue;

OxmqQueue<OrderData> queue = OxmqQueue.<OrderData>builder()
        .name("orders")
        .redisUri("redis://localhost:6379")
        .payloadClass(OrderData.class)
        .build();
```

```java [Spring Boot Injection]
import io.oxmq.spring.OxmqTemplate;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

@Service
public class OrderService {

    @Autowired
    private OxmqTemplate oxmqTemplate;

    public void createOrder(OrderData order) {
        oxmqTemplate.send("orders", order);
    }
}
```

:::

---

## 📤 Enqueuing Jobs

### 1. Single Job (`add`)

Enqueue a job with default or custom options:

```java
// Default options (immediate execution, FIFO)
Job<OrderData> job1 = queue.add("process-order", new OrderData("ord_001", 99.00));

// Custom options (delay, retries, exponential backoff, priority)
Job<OrderData> job2 = queue.add(
        "process-order",
        new OrderData("ord_002", 199.00),
        JobOptions.builder()
                .delay(Duration.ofMinutes(1))
                .attempts(3)
                .exponentialBackoff(Duration.ofSeconds(2), Duration.ofMinutes(1))
                .priority(5)
                .build()
);
```

### 2. Bulk Enqueue (`addBulk`)

When submitting hundreds or thousands of jobs simultaneously, use `addBulk`. OxMQ executes atomic batching using Redis pipelines, minimizing network roundtrips:

```java
List<JobRequest<OrderData>> batch = List.of(
        new JobRequest<>("process-order", new OrderData("ord_101", 45.00)),
        new JobRequest<>("process-order", new OrderData("ord_102", 75.00)),
        new JobRequest<>("process-order", new OrderData("ord_103", 120.00))
);

List<Job<OrderData>> createdJobs = queue.addBulk(batch);
System.out.println("Enqueued " + createdJobs.size() + " jobs in a single pipeline call.");
```

---

## 🔍 Inspecting Jobs & Queue Metrics

### Query Current Job State (`getJobState`)

Get the precise state of any job across all Redis states (`WAITING`, `ACTIVE`, `DELAYED`, `COMPLETED`, `FAILED`, `WAITING_CHILDREN`, `PAUSED`):

```java
JobState state = queue.getJobState(job.getId());
System.out.println("Current job state: " + state);

if (state.isCompleted()) {
    System.out.println("Job is already completed!");
}
```

### Request-Reply on Queue (`waitUntilFinished`)

Await job completion directly through the Queue API:

```java
// Synchronously wait for job completion with timeout
Object result = queue.waitUntilFinished(job.getId(), queueEvents, Duration.ofSeconds(15));

// Or non-blocking CompletableFuture
CompletableFuture<Object> future = queue.waitUntilFinishedAsync(job.getId(), queueEvents, Duration.ofSeconds(15));
```

### Aggregate Queue Counts (`getJobCounts`)

Retrieve a snapshot of all job counts in the queue:

```java
Map<String, Long> counts = queue.getJobCounts();
System.out.printf("Waiting: %d | Active: %d | Delayed: %d | Completed: %d | Failed: %d%n",
        counts.get("waiting"),
        counts.get("active"),
        counts.get("delayed"),
        counts.get("completed"),
        counts.get("failed")
);
```

### Retrieve Job Execution Logs (`getJobLogs`)

Retrieve chronological diagnostic logs added by workers during task execution:

```java
// Fetch the first 50 log rows
List<String> logs = queue.getJobLogs(job.getId(), 0, 50);
logs.forEach(System.out::println);
```

---

## ⏸️ Queue Lifecycle Control

### Pausing & Resuming

Pausing a queue halts worker dispatching across the entire cluster. Any newly submitted jobs accumulate safely in Redis until resumed:

```java
// Pause cluster-wide job execution
queue.pause();
System.out.println("Queue paused: " + queue.isPaused());

// Resume job processing
queue.resume();
```

### Draining Jobs (`drain`)

Removes all jobs currently waiting in the queue without removing active jobs:

```java
// Drain waiting jobs only
queue.drain(false);

// Drain both waiting and delayed jobs
queue.drain(true);
```

### Pruning Expired Jobs (`clean`)

Cleans up old completed or failed jobs based on a grace period:

```java
// Remove completed jobs older than 24 hours (limit to 5,000 per call)
long graceMs = Duration.ofHours(24).toMillis();
long cleanedCount = queue.clean(graceMs, 5000, JobState.COMPLETED);
System.out.println("Purged " + cleanedCount + " old completed jobs");
```

### Obliterating a Queue (`obliterate`)

Permanently destroys the queue, removing all associated Redis keys, sets, hashes, and streams:

```java
queue.obliterate();
```

---

## 🔄 Dynamic In-Flight Job Operations

OxMQ allows mutating jobs already submitted to Redis:

```java
String jobId = "job_456";

// Promote a delayed job to WAITING immediately
queue.promote(jobId);

// Dynamically reschedule delay
queue.changeDelay(jobId, Duration.ofMinutes(15));

// Change job priority in the waiting set
queue.changePriority(jobId, 1);

// Re-queue a failed job back to WAITING
queue.retry(jobId);

// Mutate the job payload data in-place
queue.updateData(jobId, new OrderData("ord_456", 250.00));

// Remove a specific job (and optionally cascade-delete child tasks)
queue.remove(jobId, true);

// Release a custom deduplication key early
queue.removeDeduplicationKey("custom_dedup_hash");
```
