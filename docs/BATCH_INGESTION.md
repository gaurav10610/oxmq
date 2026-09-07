# ⚡ High-Throughput Batch Dequeue & Bulk Ingestion

When ingesting high-velocity event streams (e.g., audit logs, telemetry, payment transactions) into analytical stores like **ClickHouse**, **Elasticsearch**, **PostgreSQL (COPY / JDBC Batch)**, or **Snowflake**, processing jobs one-by-one introduces severe bottlenecks:
1. **Network Round-Trip Overhead**: 10,000 jobs processed individually require 20,000+ Redis and Database roundtrips.
2. **Database Transaction Contention**: Inserting 1 record per SQL statement saturates WAL writes and database lock managers.

**OxMQ solves this with `OxmqBatchWorker`**: an atomic batch popping engine that retrieves up to $N$ jobs in **1 single Redis roundtrip** using atomic Lua scripts.

---

## 📊 Single Job vs Batch Dequeue Comparison

| Dimension | Single Job Dequeue (`OxmqWorker`) | Batch Dequeue (`OxmqBatchWorker`) |
| :--- | :--- | :--- |
| **Redis Roundtrips for 1,000 Jobs** | 1,000 requests | **10 to 20 requests** (batch size 50–100) |
| **Database Insertion Method** | `INSERT INTO tbl VALUES (...)` | `INSERT INTO tbl VALUES (...), (...), ...` |
| **Network Overhead** | High latency ($\sim 1\text{ms} \times 1,000 = 1\text{s}$) | Ultra-low latency ($\sim 1\text{ms} \times 20 = 20\text{ms}$) |
| **Throughput (Ops/sec)** | $\sim 5,000\text{ ops/s}$ | **$\ge 50,000\text{ ops/s}$** |
| **Best For** | Webhooks, LLM calls, Email sending | ClickHouse, Elasticsearch, PostgreSQL bulk inserts |

---

## 🛠️ Step-by-Step Implementation

### 1. Define Bulk Event Payload

```java
public record AuditLogEvent(
        String traceId,
        String userId,
        String action,
        String resource,
        long timestamp
) {}
```

### 2. Configure `OxmqBatchWorker`

```java
import io.oxmq.OxmqBatchWorker;
import io.oxmq.model.Job;
import java.util.List;

public class BatchIngestionWorker {
    public static void main(String[] args) {
        OxmqBatchWorker<AuditLogEvent> batchWorker = OxmqBatchWorker.<AuditLogEvent>builder()
                .queueName("audit-log-stream")
                .redisUri("redis://localhost:6379")
                .payloadClass(AuditLogEvent.class)
                .batchSize(50)             // Pop up to 50 jobs at once
                .lockDurationMs(30_000)    // 30-second lock per batch
                .pollIntervalMs(50)        // Fast polling loop
                .processor(jobs -> {
                    System.out.printf("Popped bulk batch of %d audit logs (VirtualThread: %b)%n",
                            jobs.size(), Thread.currentThread().isVirtual());

                    // Execute bulk database insert (JDBC batch, ClickHouse HTTP bulk, Elasticsearch Bulk API)
                    insertAuditLogsBulk(jobs);

                    return "INGESTED_" + jobs.size();
                })
                .build();

        batchWorker.start();
    }

    private static void insertAuditLogsBulk(List<Job<AuditLogEvent>> jobs) {
        // Example JDBC Batch Insertion:
        // try (PreparedStatement ps = conn.prepareStatement("INSERT INTO audit_logs (...) VALUES (?, ?, ?, ?)")) {
        //     for (Job<AuditLogEvent> job : jobs) {
        //         AuditLogEvent log = job.getData();
        //         ps.setString(1, log.traceId());
        //         ps.setString(2, log.userId());
        //         ps.setString(3, log.action());
        //         ps.addBatch();
        //     }
        //     ps.executeBatch();
        // }
    }
}
```

---

## 🔒 Atomic State Transitions (`moveToActiveBatch.lua`)

When `OxmqBatchWorker` requests a batch:
1. OxMQ executes `moveToActiveBatch.lua` in Redis.
2. It atomically pops up to `batchSize` job IDs from `bull:<queue>:wait`.
3. It moves all IDs to `bull:<queue>:active`, sets lock timestamps, and returns the full batch in a single multi-bulk array response.
4. When processing completes, `moveToFinishedBatch.lua` transitions all jobs in the batch to `completed` in 1 atomic operation.
