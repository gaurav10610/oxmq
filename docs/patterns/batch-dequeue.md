# Pattern: Batch Ingestion & Bulk Operations

When building high-volume telemetry ingestion, audit logging, or event warehousing, processing jobs strictly 1-by-1 can become inefficient due to database connection overhead.

OxMQ provides high-throughput **Bulk Enqueue** and **Batch Ingestion** architectures.

---

## 📥 Atomic Bulk Enqueue (`addBulk`)

When an API gateway or microservice ingests a batch of 1,000 events, enqueuing them individually requires 1,000 separate network round-trips to Redis.

Using `addBulk` uses Redis pipelining to serialize and enqueue the entire batch in a single network round-trip:

```java
List<JobRequest<AuditEvent>> events = incomingAuditLogs.stream()
        .map(log -> new JobRequest<>("audit-log", log))
        .toList();

// Enqueues all 1,000 jobs atomically
List<Job<AuditEvent>> jobs = queue.addBulk(events);
```

---

## 📦 Batch Database Ingestion Pattern

For analytical databases like **ClickHouse**, **Snowflake**, or **Elasticsearch**, inserting records in batches of 500–1,000 rows achieves significantly higher throughput than single-row `INSERT` statements.

```text
[OxMQ Queue: bull:audit-logs:wait]
               │
               ▼ (Fetch batch of up to N jobs)
    [OxMQ Batch Ingestion Worker]
               │
               ▼ (Execute multi-row bulk insert)
    [ClickHouse / Elasticsearch / PostgreSQL]
               │
               ▼ (Acknowledge batch completion)
[Batch marked COMPLETED in Redis]
```

### Spring Boot Declarative Batch Listener (`@OxmqBatchListener`)

In Spring Boot 3+, you can consume atomic batches directly with `@OxmqBatchListener`:

```java
@Component
public class ClickHouseAuditWorker {

    @Autowired
    private ClickHouseTemplate clickHouse;

    @OxmqBatchListener(queue = "audit-events", batchSize = 100)
    public void onAuditBatch(List<Job<AuditEvent>> batch) {
        List<AuditEvent> events = batch.stream().map(Job::getData).toList();
        // Perform fast JDBC or analytical DB bulk insert in 1 roundtrip
        clickHouse.batchInsert("INSERT INTO audit_logs VALUES (?, ?, ?)", events);
    }
}
```

### Memory Safeguards with `maxBatchBytes`

When jobs carry large variable-sized payloads (e.g. multi-megabyte audit documents), assembling batches solely based on item count (`batchSize`) can lead to unexpected JVM heap spikes. 

OxMQ provides the `maxBatchBytes` safeguard to enforce strict memory budgeting on batch workers:

```java
@OxmqBatchListener(
    queue = "audit-events",
    batchSize = 100,
    maxBatchBytes = 5_242_880 // 5 MB maximum cumulative payload budget
)
public void onAuditBatch(List<Job<AuditEvent>> batch) {
    clickHouse.batchInsert("INSERT INTO audit_logs VALUES (?, ?, ?)", events);
}
```

Or programmatically via `OxmqBatchWorker.builder()`:

```java
OxmqBatchWorker<AuditEvent> worker = OxmqBatchWorker.<AuditEvent>builder()
        .queueName("audit-events")
        .batchSize(100)
        .maxBatchBytes(5 * 1024 * 1024) // 5 MB byte budget
        .concurrency(2)                 // Semaphore-bounded concurrent batches
        .processor(this::processBatch)
        .build();
```

When cumulative payload bytes reach or exceed `maxBatchBytes`, the worker dispatches the batch immediately, ensuring deterministic memory bounds regardless of payload variance.

---

## 🎯 Item-Level Granular Partial Failures (`job.setAsFailed`)

In traditional batch workers, if a single record fails during processing (e.g. invalid document schema, third-party 400 Bad Request), throwing an exception causes the **entire batch of 100 jobs to fail and retry**, duplicating processing for the 99 records that succeeded.

OxMQ introduces **Granular Item-Level Partial Failures**:

```java
@OxmqBatchListener(queue = "document-indexing", batchSize = 50)
public void onDocumentBatch(List<Job<DocumentPayload>> batch) {
    for (Job<DocumentPayload> job : batch) {
        try {
            indexDocument(job.getData());
        } catch (ValidationException e) {
            // Mark ONLY this specific job as failed!
            // It will be recorded as FAILED in Redis with its stack trace.
            job.setAsFailed(e);
        }
    }
    // All non-failed jobs in the batch automatically succeed and complete!
}
```

### Pure Java API

```java
OxmqBatchWorker<DocumentPayload> worker = OxmqBatchWorker.<DocumentPayload>builder()
        .queueName("document-indexing")
        .batchSize(50)
        .processor(batch -> {
            for (Job<DocumentPayload> job : batch) {
                if (isCorrupt(job.getData())) {
                    job.setAsFailed(new IllegalArgumentException("Corrupt document payload"));
                } else {
                    processItem(job.getData());
                }
            }
            return "BATCH_PROCESSED";
        })
        .build();
```

When partial failures occur:
- Jobs flagged with `job.setAsFailed(err)` are transitioned to `failed` state with the individual error message and truncated stack trace.
- All other jobs in the batch are transitioned to `completed` state with the returned result.
- Metrics record precise counts for completed vs failed items.

---

## ⏳ Batch Accumulation Buffering (`minBatchSize` & `batchTimeout`)

When background job arrival rates fluctuate, popping batches immediately can result in tiny batches of 1 or 2 items during quiet periods, defeating the purpose of bulk operations.

OxMQ provides built-in **Accumulation Buffering**:

```java
@OxmqBatchListener(
    queue = "audit-events",
    batchSize = 100,           // Maximum batch size target
    minBatchSize = 10,         // Wait until at least 10 jobs arrive
    batchTimeoutMs = 250       // BUT dispatch early if 250ms elapsed
)
public void onAuditBatch(List<Job<AuditEvent>> batch) {
    bulkInsertToClickHouse(batch);
}
```

Or programmatically:

```java
OxmqBatchWorker<AuditEvent> worker = OxmqBatchWorker.<AuditEvent>builder()
        .queueName("audit-events")
        .batchSize(100)
        .minBatchSize(10)
        .batchTimeout(Duration.ofMillis(250))
        .processor(this::processBatch)
        .build();
```

---

## 🏢 Multi-Tenant Homogeneous Batches (`groupAffinity`)

When processing multi-tenant data, mixing records from different tenants in a single batch violates multi-tenant database isolation (e.g. separate schemas per tenant or tenant-scoped transactions).

Enable **Group Affinity** to guarantee that every batch delivered to your listener contains jobs from only **ONE** virtual group:

```java
@OxmqBatchListener(
    queue = "tenant-analytics",
    batchSize = 25,
    groupAffinity = true,      // GUARANTEE: All items belong to the same tenant!
    groupConcurrency = 2       // Max 2 simultaneous batches per tenant
)
public void onTenantBatch(List<Job<Event>> batch) {
    String tenantId = batch.get(0).getGroupKey();
    tenantScopedDatabase.bulkInsert(tenantId, batch);
}
```

---

## 🔗 Accessing Batched Jobs via `job.getBatch()` & Worker Callbacks

When writing domain logic inside batch workers, you can access the sibling jobs directly from any individual `Job` instance (mirroring BullMQ Pro's `job.getBatch()`):

```java
// Inside batch processing logic:
Job<AuditEvent> firstJob = batch.get(0);

// Retrieve all jobs belonging to this batch execution
List<Job<AuditEvent>> fullBatch = firstJob.getBatch();
System.out.println("Processing batch of " + fullBatch.size() + " items");
```

### Fluent Event Callbacks for Batches
`OxmqBatchWorker` provides lifecycle listener callbacks for monitoring batch executions:

```java
batchWorker
    .onBatchActive(jobs -> log.info("Started processing batch of {} jobs", jobs.size()))
    .onJobCompleted((job, result) -> log.info("Job {} completed in batch", job.getId()))
    .onJobFailed((job, err) -> log.warn("Job {} failed in batch: {}", job.getId(), err.getMessage()))
    .onBatchCompleted((jobs, result) -> log.info("Batch of {} jobs completed", jobs.size()))
    .onBatchFailed((jobs, err) -> log.error("Entire batch failed", err));
```

---

## ⚡ Empirical Batch Worker Performance

In our empirical benchmarks, `OxmqBatchWorker` reached up to **2,918.9 jobs/second** (Java 17 LTS) and **2,886.8 jobs/second** (Java 21 LTS) peak throughput—a **2.6x multiplier** over single-job workers:

* **Production Sweet Spot (`batchSize = 100`)**: Delivers **2,853 – 2,896 jobs/second** with per-batch dispatch latency under **35 ms**.
* **Redis Network Efficiency**: Eliminates 90–99% of round-trips compared to 1-by-1 popping.
* **Cost / Node**: ~672 – 693 Bytes in Redis.
* **Peak JVM Heap**: Under 80 MB with Virtual Threads (and < 70 MB on Java 17).

For complete benchmark tables, sizing matrices, and inflection point analyses, see the [Scale & Performance Benchmarks Guide](/operations/benchmarks).
