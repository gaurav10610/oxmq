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

---

## ⚡ Empirical Batch Worker Performance

In our empirical benchmarks, `OxmqBatchWorker` reached up to **2,918.9 jobs/second** (Java 17 LTS) and **2,886.8 jobs/second** (Java 21 LTS) peak throughput—a **2.6x multiplier** over single-job workers:

* **Production Sweet Spot (`batchSize = 100`)**: Delivers **2,853 – 2,896 jobs/second** with per-batch dispatch latency under **35 ms**.
* **Redis Network Efficiency**: Eliminates 90–99% of round-trips compared to 1-by-1 popping.
* **Cost / Node**: ~672 – 693 Bytes in Redis.
* **Peak JVM Heap**: Under 80 MB with Virtual Threads (and < 70 MB on Java 17).

For complete benchmark tables, sizing matrices, and inflection point analyses, see the [Scale & Performance Benchmarks Guide](/operations/benchmarks).
