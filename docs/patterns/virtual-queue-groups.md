# Virtual Queue Groups & Fair Multi-Tenant Scheduling

> **100% Free & Open-Source (Apache-2.0)** — Unlike BullMQ where Groups are gated behind a commercial **BullMQ Pro** paid subscription, OxMQ includes enterprise-grade Virtual Queue Groups natively in core with **zero licensing fees, zero pro tier, and zero commercial paywalls**.

Virtual Queue Groups provide fine-grained multi-tenant isolation, fair round-robin scheduling, per-tenant concurrency caps, and dynamic rate limiting—all within a single Redis queue without spawning thousands of individual physical queues.

---

## 1. Why Virtual Queue Groups?

In multi-tenant SaaS platforms, background job processing suffers from two major challenges when using standard FIFO queues:

1. **The "Noisy Neighbor" Problem (Starvation):** If Tenant A suddenly enqueues 50,000 document exports, Tenant B's urgent password-reset job is trapped behind 50,000 jobs in the FIFO queue.
2. **Physical Queue Sprawl:** Spawning a distinct Redis queue for each of 10,000 tenants creates massive Redis key overhead, connection pool exhaustion, and complex consumer lifecycle polling loops.

```
Without Virtual Groups (FIFO Queue - Tenant B is Starved):
[ A ][ A ][ A ][ A ][ A ][ A ][ A ][ A ][ A ] ... [ A ][ B (Starved!) ]

With OxMQ Virtual Queue Groups (Fair Round-Robin Circular Rotation):
[ A ][ B ][ C ][ A ][ B ][ C ][ A ][ B ][ C ] -> Workers process interleaved!
```

OxMQ Virtual Queue Groups solve this natively by maintaining sub-queues grouped by a virtual identifier (e.g., `tenantId`, `userId`, `orgId`) with $O(1)$ fair circular scheduling across groups.

---

## 2. Redis Data Architecture & Upstream Isolation

OxMQ implements virtual groups using Redis primitive structures with strict isolation from standard BullMQ keys. To guarantee **100% clean, conflict-free periodic synchronization with upstream BullMQ**, all group Lua scripts are maintained in `lua/extensions/`:

```
bull:<queue>:groups:waiting       -> LIST of group IDs waiting for processing (Circular RPOPLPUSH)
bull:<queue>:groups:active:count  -> HASH { groupId: activeJobCount }
bull:<queue>:groups:concurrency   -> HASH { groupId: dynamicConcurrencyOverride }
bull:<queue>:groups:maxed         -> SET of group IDs whose concurrency cap has been reached
bull:<queue>:groups:limited       -> ZSET of throttled group IDs (score = expiration epoch ms)
bull:<queue>:groups:paused        -> SET of administratively paused group IDs
bull:<queue>:groups:<groupId>     -> LIST of pending FIFO jobs for that virtual group
bull:<queue>:groups:<groupId>:prioritized -> ZSET of prioritized jobs for that group
```

### $O(1)$ Fair Round-Robin Circular Scheduling
When an OxMQ worker dequeues from a queue with virtual groups:
1. The Lua script executes `RPOPLPUSH bull:<queue>:groups:waiting bull:<queue>:groups:waiting`.
2. It inspects the selected group for paused, rate-limited, or maxed concurrency status.
3. It pops the next job from that group's sub-queue (`ZPOPMIN` prioritized or `RPOP` FIFO).
4. Group active count is incremented in `groups:active:count`.
5. If the group has reached its `groupConcurrency` cap, it is moved from `groups:waiting` to `groups:maxed`.
6. When a job finishes or fails, `decrementGroupActive` decrements the counter and immediately restores maxed groups back to `groups:waiting`.

This design achieves guaranteed fair rotation in $O(1)$ time complexity, regardless of whether you have 10 or 100,000 active tenants.

---

## 3. Pure Java API Usage

### Enqueueing Jobs to a Virtual Group

You can attach a job to a virtual group simply by specifying `.group(groupId)` on `JobOptions`:

```java
import io.oxmq.OxmqQueue;
import io.oxmq.model.GroupOptions;
import io.oxmq.model.JobOptions;

OxmqQueue<String> queue = OxmqQueue.<String>builder()
        .name("document-exports")
        .redisUri("redis://localhost:6379")
        .build();

// Enqueue with simple group ID
queue.add("export-csv", "{\"reportId\": 402}", 
        JobOptions.builder()
                .group("tenant-alpha")
                .build());

// Enqueue with bounded backlog (maxSize)
// If tenant-beta already has 100 pending jobs, this throws GroupMaxSizeExceededException
queue.add("export-pdf", "{\"reportId\": 403}", 
        JobOptions.builder()
                .group(GroupOptions.builder()
                        .id("tenant-beta")
                        .maxSize(100)
                        .build())
                .build());
```

### Intra-Group Priority Queuing

In enterprise multi-tenant systems, individual tenants often need urgent or VIP jobs to jump ahead of standard background jobs **within their own virtual group**, without unfairly starving other tenants.

OxMQ Virtual Queue Groups support native **Intra-Group Priority**:

```java
// Standard priority job for tenant-alpha
queue.add("report-standard", "{\"reportId\": 101}",
        JobOptions.builder()
                .group(GroupOptions.builder()
                        .id("tenant-alpha")
                        .priority(10) // Lower priority number = processed first!
                        .build())
                .build());

// Urgent VIP job for tenant-alpha (jumps ahead within tenant-alpha's queue)
queue.add("report-urgent", "{\"reportId\": 999}",
        JobOptions.builder()
                .group(GroupOptions.builder()
                        .id("tenant-alpha")
                        .priority(1)  // Top priority (1 < 10)
                        .build())
                .build());

// Query counts per priority tier for a specific group
Map<Integer, Long> priorityCounts = queue.getCountsPerPriorityForGroup("tenant-alpha");
System.out.println("Pending priority 1 jobs: " + priorityCounts.get(1));
System.out.println("Pending priority 10 jobs: " + priorityCounts.get(10));
```

Jobs with a `group.priority` are stored in `bull:<queue>:groups:<groupId>:prioritized` (a Redis ZSET scored by priority). During round-robin group selection, prioritized jobs are popped first (`ZPOPMIN`) before standard FIFO jobs (`RPOP`).

### Processing with Worker Concurrency Caps

Configure a worker with overall cluster concurrency and a per-group concurrency limit:

```java
import io.oxmq.OxmqWorker;

OxmqWorker<String> worker = OxmqWorker.<String>builder()
        .queueName("document-exports")
        .redisUri("redis://localhost:6379")
        .concurrency(20)          // Worker takes up to 20 jobs simultaneously
        .groupConcurrency(2)      // BUT no single tenant can consume more than 2 slots!
        .processor(job -> {
            String tenantId = job.getGroupKey();
            System.out.println("Processing report for tenant: " + tenantId);
            generateDocument(job.getData());
            return "SUCCESS";
        })
        .build();

worker.start();
```

---

## 4. Homogeneous Batch Processing with Group Affinity

When processing jobs in batches (e.g. bulk database inserts or micro-batches for vector indexing), mixing records from different tenants in the same batch breaks multi-tenant database isolation.

OxMQ's `OxmqBatchWorker` provides **Strict Group Affinity**:

```java
import io.oxmq.OxmqBatchWorker;

OxmqBatchWorker<String> batchWorker = OxmqBatchWorker.<String>builder()
        .queueName("document-exports")
        .redisUri("redis://localhost:6379")
        .batchSize(10)            // Max 10 jobs per batch
        .groupAffinity(true)      // GUARANTEE: All jobs in every batch belong to the SAME tenant!
        .groupConcurrency(1)      // Max 1 concurrent batch per tenant
        .concurrency(5)           // Up to 5 batches processed simultaneously
        .processor(batch -> {
            String tenantId = batch.get(0).getGroupKey();
            System.out.printf("Executing homogeneous batch of %d jobs for %s%n", batch.size(), tenantId);
            bulkInsertDocuments(tenantId, batch);
            return "BATCH_OK";
        })
        .build();

batchWorker.start();
```

---

## 5. Administrative Control: Pause, Rate-Limit, Delete

### Dynamic Cluster-Wide Group Rate Limiting
If a tenant exceeds their external API quotas (e.g., received an HTTP 429 Too Many Requests from an upstream provider), you can throttle only that tenant cluster-wide without impacting any other tenant:

```java
// Throttles tenant-alpha across all worker nodes for 10 seconds
queue.setGroupRateLimit("tenant-alpha", Duration.ofSeconds(10));

// Or directly from inside a worker processor:
worker.rateLimitGroup("tenant-alpha", Duration.ofSeconds(10));
```

### Pausing and Resuming a Virtual Group
Halt job execution for an individual tenant during tenant database migration or maintenance:

```java
// Pause tenant-alpha; jobs will queue up but workers will skip them
queue.pauseGroup("tenant-alpha");

// Resume processing once migration completes
queue.resumeGroup("tenant-alpha");
```

### Deleting a Virtual Group
Purge all pending jobs for an offboarded tenant in a single atomic Redis roundtrip:

```java
long deletedCount = queue.deleteGroup("tenant-alpha");
System.out.println("Purged " + deletedCount + " jobs from tenant-alpha backlog");
```

### Dynamic Per-Group Concurrency Overrides

While workers define a default baseline `groupConcurrency` (e.g., 2 concurrent jobs per tenant), production platforms frequently need to dynamically adjust concurrency caps for specific tenants based on subscription tiers (e.g. Free Tier = 1, Enterprise Tier = 10, VIP Customer = 25) without restarting worker pods.

OxMQ stores dynamic overrides in Redis hash `bull:<queue>:groups:concurrency`. When workers evaluate group saturation, dynamic overrides supersede worker defaults:

```java
// Set an administrative concurrency override of 10 for enterprise tenant
queue.setGroupConcurrency("tenant-enterprise", 10);

// Read back the currently active concurrency override
int activeConcurrency = queue.getGroupConcurrency("tenant-enterprise"); // returns 10

// Reset tenant back to worker's default groupConcurrency cap
queue.removeGroupConcurrency("tenant-enterprise");
```

In Spring Boot, you can manage these overrides via `OxmqTemplate`:

```java
@Autowired
private OxmqTemplate oxmqTemplate;

public void upgradeTenantTier(String tenantId, int tierConcurrency) {
    oxmqTemplate.setGroupConcurrency("document-exports", tenantId, tierConcurrency);
}
```

### Inspecting and Querying Groups
Paginate through active, limited, paused, or maxed groups for administrative dashboards:

```java
// List up to 20 waiting groups
List<ExtensionScripts.GroupEntry> waiting = queue.getGroupsByStatus("waiting", 0, 20);

// Inspect jobs waiting for a specific group
List<Job<String>> tenantJobs = queue.getGroupJobs("tenant-alpha", 0, 50);
```

---

## 6. Spring Boot 3+ Starter Integration

### Annotating Listeners with `groupConcurrency`
In Spring Boot, use `@OxmqListener` with `groupConcurrency`:

```java
import io.oxmq.model.Job;
import io.oxmq.spring.annotation.OxmqListener;
import org.springframework.stereotype.Component;

@Component
public class MultiTenantExportListener {

    @OxmqListener(
        queue = "document-exports",
        concurrency = 50,           // Total cluster throughput
        groupConcurrency = 3        // Max 3 concurrent jobs per tenant
    )
    public void processExport(Job<ExportRequest> job) {
        String tenantId = job.getGroupKey();
        ExportRequest payload = job.getData();
        // Guaranteed: Never more than 3 threads simultaneously executing for tenantId!
    }
}
```

### Spring Batch Listener with Group Affinity

```java
import io.oxmq.model.Job;
import io.oxmq.spring.annotation.OxmqBatchListener;
import org.springframework.stereotype.Component;
import java.util.List;

@Component
public class BulkIndexingBatchListener {

    @OxmqBatchListener(
        queue = "vector-indexing",
        batchSize = 25,
        minBatchSize = 5,
        batchTimeoutMs = 250,
        groupAffinity = true,       // Homogeneous batch per tenant
        groupConcurrency = 1,
        concurrency = 10
    )
    public void processBatch(List<Job<VectorItem>> batch) {
        String tenant = batch.get(0).getGroupKey();
        vectorDatabase.bulkUpsert(tenant, batch);
    }
}
```

### Enqueueing via `OxmqTemplate`

```java
@Autowired
private OxmqTemplate oxmqTemplate;

public void scheduleExport(String tenantId, ExportRequest request) {
    // Send job directly into tenant virtual group
    oxmqTemplate.sendGroup("document-exports", "pdf-export", tenantId, request);
}
```

---

## 7. Performance Characteristics

- **Enqueue Overhead:** $< 0.4 \text{ ms}$ per job ($O(1)$ hash and list operations).
- **Fair Rotation Overhead:** Zero difference in throughput compared to flat FIFO queues; circular `RPOPLPUSH` operates entirely within Redis memory.
- **Scalability:** Verified up to 100,000 distinct virtual groups on a single Redis instance without memory spikes.
- **Zero Pro Tier Restriction:** Available as standard, built-in functionality in `oxmq-core` and `oxmq-spring-boot-starter`.
