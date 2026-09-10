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

### High-Throughput Ingestion Example:

```java
@Component
public class ClickHouseAuditWorker {

    private final List<AuditEvent> buffer = new CopyOnWriteArrayList<>();
    private final ScheduledExecutorService flusher = Executors.newSingleThreadScheduledExecutor();

    @Autowired
    private ClickHouseTemplate clickHouse;

    @PostConstruct
    public void init() {
        // Flush buffer every 1 second if size threshold not reached
        flusher.scheduleAtFixedRate(this::flushBuffer, 1, 1, TimeUnit.SECONDS);
    }

    @OxmqListener(queue = "audit-events", concurrency = 20)
    public void onAuditEvent(Job<AuditEvent> job) {
        buffer.add(job.getData());
        if (buffer.size() >= 500) {
            flushBuffer();
        }
    }

    private synchronized void flushBuffer() {
        if (buffer.isEmpty()) return;
        List<AuditEvent> batch = new ArrayList<>(buffer);
        buffer.clear();

        // Perform fast JDBC bulk insert
        clickHouse.batchInsert("INSERT INTO audit_logs VALUES (?, ?, ?)", batch);
    }
}
```
