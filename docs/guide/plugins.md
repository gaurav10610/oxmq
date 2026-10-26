# Plugins & Interceptors SPI

OxMQ provides a non-intrusive, zero-overhead **Interceptor & Plugin Architecture** via `JobInterceptor`.

It allows enterprise applications to intercept jobs at every stage of the execution lifecycle—enforcing distributed multi-tenant rate limits, circuit-breaking external HTTP 429 errors, governing priority quotas, propagating distributed tracing context (MDC/OpenTelemetry), and emitting audit events—**without modifying business worker code and with zero changes to upstream BullMQ Lua scripts**.

---

## 🏛️ Interceptor Lifecycle & Architecture

The worker execution lifecycle in `OxmqWorker` executes an interceptor chain around every job dispatch:

```mermaid
sequenceDiagram
    autonumber
    participant R as Redis Cluster
    participant W as OxmqWorker (Virtual Thread)
    participant I as JobInterceptor Pipeline
    participant J as Business Processor
    participant M as Micrometer / Prometheus

    W->>R: Fetch Job (moveToActive-11.lua)
    R-->>W: Job<T> (Active State)
    
    rect rgb(240, 248, 255)
        Note over W,I: Pre-Execution Phase
        W->>I: beforeProcess(job)
        alt InterceptorDecision.delay(delayMs, true)
            I->>M: Increment delay counter
            I-->>W: DELAY (delayMs, skipAttempt=true)
            W->>R: moveToDelayed-11.lua (non-destructive)
            Note over W: Worker claims next job immediately
        else InterceptorDecision.reject(reason)
            I-->>W: REJECT (reason)
            W->>R: moveToFailed-9.lua (JobRejectedException)
        else InterceptorDecision.proceed()
            I-->>W: PROCEED
        end
    end

    rect rgb(245, 255, 245)
        Note over W,J: Execution Phase
        W->>J: process(job)
        alt Success
            J-->>W: Return Value
            W->>I: afterProcess(job, duration)
            W->>R: moveToFinished-14.lua
        else Exception Thrown (e.g. HTTP 429)
            J-->>W: Throws RateLimitCooldownException
            W->>I: onError(job, error, duration)
            alt ErrorDecision.delay(delayMs, skipAttempt=true)
                I->>R: SET cluster cooldown key (TTL)
                I->>M: Increment sentinel delay counter
                W->>R: moveToDelayed-11.lua (skipAttempt=true)
            else ErrorDecision.bubbleUp()
                W->>R: Standard BullMQ Retry / DLQ
            end
        end
    end
```

> [!IMPORTANT] Upstream Lua Script Invariance
> All OxMQ plugins use standard BullMQ Lua primitives (`moveToDelayed-*.lua` with `skipAttempt=true`). No upstream Lua scripts are modified or diverged, preserving **100% byte-for-byte polyglot compatibility** with Node.js and Python BullMQ workers.

---

## 🧩 Native Built-in Plugins

OxMQ bundles five production-grade enterprise plugins out of the box in `io.oxmq.plugin`:

| Plugin | Primary Enterprise Purpose | Key Metrics & State Emitted |
| :--- | :--- | :--- |
| **`GroupRateLimiterPlugin`** | Dynamic sliding-window token bucket partitioned by custom group key (tenant, customer, license tier). | `oxmq.plugin.ratelimiter.yields` |
| **`RateLimitSentinelPlugin`** | Distributed circuit-breaker sentinel for external HTTP 429/503 `Retry-After` cooldowns across all cluster nodes. | `oxmq.plugin.sentinel.delays`<br/>`oxmq.plugin.sentinel.cooldowns` |
| **`PriorityQuotaGovernorPlugin`** | Headroom-based yielding: lower-priority background sweeps automatically defer execution to reserve quota for real-time priority jobs. | `oxmq.plugin.governor.yields` |
| **`AdaptiveRateLimiterPlugin`** | AIMD dynamic capacity controller: auto-downgrades on 429/503 errors, locks against oscillation, and upgrades on clean execution windows. | Synchronized in Redis hash `oxmq:adaptive:limits` |
| **`CheckpointStatePlugin`** | Distributed cursor & delta token persistence in Redis (`oxmq:checkpoint:<namespace>`), thread-local context injection, and resume-on-restart. | Thread-local context + Redis checkpoint hash |

---

## 1. `GroupRateLimiterPlugin` (Multi-Tenant Token Bucket)

Enforces dynamic or static rate limits per tenant/group across all distributed cluster nodes using an atomic Redis `INCR` + `EXPIRE` pipeline.

### Standalone Pure Java Configuration

```java
import io.oxmq.OxmqWorker;
import io.oxmq.plugin.GroupRateLimiterPlugin;
import io.oxmq.redis.RedisConnectionManager;
import java.time.Duration;

// 1. Instantiate the plugin
GroupRateLimiterPlugin rateLimiter = GroupRateLimiterPlugin.builder()
        .connectionManager(connectionManager)
        .keyPrefix("oxmq:limiter:tenant")
        .groupKeyResolver(job -> ((TenantPayload) job.getData()).getTenantId())
        .dynamicLimit(tenantId -> licenseTierService.getBurstLimit(tenantId)) // e.g. 50-200 req/sec
        .windowDuration(Duration.ofSeconds(1))
        .retryDelayMs(150)
        .meterRegistry(meterRegistry) // Optional Micrometer registry
        .build();

// 2. Attach directly to worker
OxmqWorker<TenantPayload> worker = OxmqWorker.<TenantPayload>builder()
        .queueName("drive-sync")
        .redisUri("redis://localhost:6379")
        .addInterceptor(rateLimiter)
        .processor(job -> syncDrive(job.getData()))
        .build();

worker.start();
```

### Spring Boot 3 Auto-Configuration

In Spring Boot, simply declare the plugin as a `@Bean`. The starter automatically discovers all `JobInterceptor` beans and injects them into all `@OxmqListener` and `@OxmqBatchListener` workers:

```java
@Configuration
public class OxmqPluginConfiguration {

    @Bean
    public GroupRateLimiterPlugin groupRateLimiter(
            RedisConnectionManager connectionManager,
            LicenseTierService licenseTierService,
            MeterRegistry meterRegistry
    ) {
        return GroupRateLimiterPlugin.builder()
                .connectionManager(connectionManager)
                .keyPrefix("oxmq:limiter:tenant")
                .groupKeyResolver(job -> {
                    if (job.getData() instanceof TenantScopedPayload tsp) {
                        return tsp.tenantId();
                    }
                    return "default";
                })
                .dynamicLimit(tenantId -> licenseTierService.getAllowedRequestsPerSecond(tenantId))
                .windowDuration(Duration.ofSeconds(1))
                .retryDelayMs(150)
                .meterRegistry(meterRegistry)
                .build();
    }
}
```

### Runtime Headroom Inspection APIs

You can query token consumption and remaining headroom programmatically at any time:

```java
// Current usage within active window
long currentCount = rateLimiter.getCurrentUsage("tenant-acme");

// Remaining tokens before exhaustion
long remaining = rateLimiter.getRemainingTokens("tenant-acme");

// Headroom percentage (0.0 to 1.0)
double headroomPercent = rateLimiter.getRemainingHeadroomPercent("tenant-acme");
```

---

## 2. `RateLimitSentinelPlugin` (External 429 Cooldown Sentinel)

When calling third-party SaaS APIs (such as Microsoft Graph, Salesforce, OpenAI, Claude, or Stripe), downstream services will respond with HTTP 429 Too Many Requests and a `Retry-After: N` header when rate limits are exceeded.

Without a sentinel, every worker thread across your cluster will continue attempting HTTP calls, resulting in cascading failures, blocked threads, and severe API bans.

### How It Works:
1. When any worker encounters an HTTP 429/503 response, it throws [`RateLimitCooldownException`](file:///Users/gaurav/git/oxmq/oxmq-core/src/main/java/io/oxmq/exception/RateLimitCooldownException.java).
2. The `RateLimitSentinelPlugin` intercepts the exception in `onError`:
   - It sets a distributed cooldown flag in Redis with a TTL matching `Retry-After`: `SET oxmq:sentinel:tenant:{tenantId} "1" EX {seconds}`.
   - It defers the failed job via `moveToDelayed` with `skipAttempt=true`, preserving all retry attempts.
3. In subsequent runs, `beforeProcess()` checks the distributed sentinel:
   - Any worker on **any cluster node** processing jobs for that tenant immediately defers the job without making the external HTTP call.

### Spring Boot Configuration

```java
@Bean
public RateLimitSentinelPlugin rateLimitSentinel(
        RedisConnectionManager connectionManager,
        MeterRegistry meterRegistry
) {
    return RateLimitSentinelPlugin.builder()
            .connectionManager(connectionManager)
            .keyPrefix("oxmq:sentinel:tenant")
            .groupKeyResolver(job -> ((TenantScopedPayload) job.getData()).tenantId())
            .errorMatcher(t -> t instanceof RateLimitCooldownException)
            .meterRegistry(meterRegistry)
            .build();
}
```

### Worker Implementation with HTTP Client

```java
@Component
public class GraphDriveWorker {

    @Autowired
    private HttpClient httpClient;

    @OxmqListener(queue = "drive-sync", concurrency = 50)
    public void processSync(Job<DriveSyncPayload> job) throws Exception {
        HttpRequest request = HttpRequest.newBuilder()
                .uri(URI.create("https://graph.microsoft.com/v1.0/drives/" + job.getData().driveId()))
                .GET()
                .build();

        HttpResponse<String> response = httpClient.send(request, HttpResponse.BodyHandlers.ofString());

        if (response.statusCode() == 429 || response.statusCode() == 503) {
            long retryAfterSec = response.headers()
                    .firstValueAsLong("Retry-After")
                    .orElse(5L);

            // Throws cooldown exception -> trips distributed cluster-wide sentinel!
            throw new RateLimitCooldownException(
                    Duration.ofSeconds(retryAfterSec), 
                    job.getData().tenantId()
            );
        }

        // Process response payload...
    }
}
```

---

## 3. `PriorityQuotaGovernorPlugin` (Headroom-Based Yielding)

In enterprise data platforms, low-priority background sweeps (e.g. historical crawls, bulk malware audits, compliance reconciliations at Priority 10) can easily saturate a tenant's API quota, starving real-time interactive user actions (e.g. user webhooks, checkout submissions, live delta syncs at Priority 1).

The **`PriorityQuotaGovernorPlugin`** enforces headroom preservation:
- It monitors remaining quota headroom: `headroom = remainingTokens / limit`.
- If headroom drops below the configured safety threshold (e.g., 25% remaining), low-priority jobs (`priority > priorityThreshold`) are non-destructively yielded back to Redis delayed state.
- Strictly protected high-priority jobs (`priority <= priorityThreshold`) continue executing uninterrupted.

### Spring Boot Configuration

```java
@Bean
public PriorityQuotaGovernorPlugin priorityQuotaGovernor(
        GroupRateLimiterPlugin rateLimiter,
        MeterRegistry meterRegistry
) {
    return PriorityQuotaGovernorPlugin.builder()
            .rateLimiter(rateLimiter)
            .groupKeyResolver(job -> ((TenantScopedPayload) job.getData()).tenantId())
            .headroomYieldThreshold(0.25) // Reserve 25% quota buffer for high-priority traffic
            .priorityThreshold(1)         // Priority <= 1 jobs are strictly protected!
            .yieldDelayMs(200)            // Yielded jobs defer 200ms before re-checking
            .meterRegistry(meterRegistry)
            .build();
}
```

---

## 4. `AdaptiveRateLimiterPlugin` (AIMD Dynamic Capacity Controller)

When interacting with cloud APIs or downstream microservices with dynamic quotas, static rate limits either underutilize available bandwidth or trigger severe cascading HTTP 429/503 throttle storms.

The **`AdaptiveRateLimiterPlugin`** brings TCP-style **AIMD (Additive Increase, Multiplicative Decrease)** capacity management natively into OxMQ:

### Core Mechanics & Mathematical Model:
1. **Multiplicative Decrease on Error / Throttling**:
   When an external request fails due to rate limits or transient server faults (matching `errorMatcher`), the capacity limit is halved:
   > **Multiplicative Decrease:** `L(t+1) = max(L_min, ⌊L(t) × β⌋)` *(default β = 0.5)*
2. **Oscillation Damping Lock (`dampingWindowMs`)**:
   Downgrades immediately trigger a freeze window (e.g. 30–60 seconds). During this lock, all upward capacity adjustments are frozen, preventing rapid upgrade/downgrade flapping ("hunting" or "yo-yo" effect).
3. **Additive Increase on Sustained Clean Execution**:
   When a group key sustains execution without any throttling errors for a configurable window (`cleanWindowsForUpgrade`), capacity increases additively:
   > **Additive Increase:** `L(t+1) = min(L_max, L(t) + α)` *(default α = +25 RUs)*
4. **Static Overrides Pinning**:
   Operators can pin any tenant to a fixed limit (`setStaticLimit`), bypassing AIMD adjustments for guaranteed provisioning.
5. **Distributed Redis State Synchronization**:
   Synchronizes limits across all distributed cluster pods via Redis hash `oxmq:adaptive:limits`, with automatic local in-memory fallback.
6. **Direct Limiter Integration (`asLimitResolver`)**:
   Seamlessly exposes `.asLimitResolver()` to supply dynamic limits directly to `GroupRateLimiterPlugin`.

### Spring Boot 3 Configuration

```java
@Configuration
public class OxmqAdaptiveLimiterConfiguration {

    @Bean
    public AdaptiveRateLimiterPlugin adaptiveRateLimiter(RedisConnectionManager cm) {
        return AdaptiveRateLimiterPlugin.builder()
                .connectionManager(cm)
                .redisStateKey("oxmq:adaptive:limits")
                .groupKeyResolver(job -> ((TenantScopedPayload) job.getData()).tenantId())
                .defaultLimit(100)                     // Default baseline limit (RUs/s)
                .minLimit(25)                          // Conservative capacity floor
                .maxLimit(2000)                        // Enterprise ceiling limit
                .multiplicativeDecreaseFactor(0.5)     // Halve capacity on 429/503
                .additiveIncreaseStep(25)              // Step up by 25 RUs on clean windows
                .cleanWindowsForUpgrade(2)             // Require 2 consecutive clean windows
                .dampingWindowMs(30_000L)              // Freeze upgrades for 30s after cut
                .errorMatcher(t -> t instanceof RateLimitCooldownException 
                                || t.getMessage().contains("429")
                                || t.getMessage().contains("503"))
                .build();
    }

    @Bean
    public GroupRateLimiterPlugin groupRateLimiter(
            RedisConnectionManager cm,
            AdaptiveRateLimiterPlugin adaptiveLimiter,
            MeterRegistry registry
    ) {
        return GroupRateLimiterPlugin.builder()
                .connectionManager(cm)
                .groupKeyResolver(job -> ((TenantScopedPayload) job.getData()).tenantId())
                .dynamicLimit(adaptiveLimiter.asLimitResolver()) // Dynamically resolved!
                .windowDuration(Duration.ofSeconds(1))
                .retryDelayMs(100)
                .meterRegistry(registry)
                .build();
    }
}
```

### Runtime Inspection & Management APIs
```java
// Query current effective capacity for a tenant
int currentLimit = adaptiveLimiter.getLimit("tenant-apex");

// Query damping lock state
boolean isDamped = adaptiveLimiter.isDamped("tenant-apex");

// Pin tenant to a static override (e.g. VIP account)
adaptiveLimiter.setStaticLimit("tenant-apex", 1500);

// Restore dynamic AIMD adjustments
adaptiveLimiter.clearStaticLimit("tenant-apex");
```

---

## 5. `CheckpointStatePlugin` (Distributed Cursor & Delta Checkpointing)

In multi-tenant ingestion pipelines (such as Microsoft Graph delta queries, database CDC replication, or S3 object bucket sweeps), worker pods may crash or be terminated by Kubernetes autoscalers mid-crawl.

Without distributed checkpoints, restarting workers re-scans drives from scratch, resulting in **millions of duplicate items, wasted database writes, and quota exhaustion**.

The **`CheckpointStatePlugin`** provides distributed, transactional checkpointing backed by Redis:

### How It Works:
1. **Distributed State in Redis**:
   Stores committed cursors/delta tokens in Redis hash `oxmq:checkpoint:<namespace>` keyed by partition (e.g. `tenantId:driveId`).
2. **Automatic Thread-Local Context Injection**:
   In `beforeProcess()`, the plugin fetches the latest committed checkpoint from Redis and injects it into thread-local context:
   ```java
   String lastDeltaToken = CheckpointStatePlugin.getCurrentCheckpoint();
   ```
3. **Declarative Commit via `CheckpointSupplier`**:
   If the job payload implements `CheckpointSupplier`, `afterProcess()` automatically commits the updated checkpoint to Redis:
   ```java
   public interface CheckpointSupplier {
       String getCheckpointPartitionKey();
       String getCheckpointValue();
   }
   ```
4. **Crash Resumption Guarantee**:
   If a worker pod terminates midway, the next worker claims the job, retrieves the exact last committed checkpoint, and resumes scanning without duplicating previously inspected files.

### Standalone & Spring Boot Configuration

```java
@Bean
public CheckpointStatePlugin checkpointStatePlugin(RedisConnectionManager cm) {
    return CheckpointStatePlugin.builder()
            .connectionManager(cm)
            .namespace("m365-delta-sync")
            .partitionKeyResolver(job -> {
                if (job.getData() instanceof DriveSyncPayload dsp) {
                    return dsp.tenantId() + ":" + dsp.driveId();
                }
                return job.getId();
            })
            .build();
}
```

### Worker Usage Example
```java
@Component
public class DriveDeltaSyncWorker {

    @OxmqListener(queue = "drive-delta-sync-queue", concurrency = 20)
    public DriveSyncPayload processDeltaSync(Job<DriveSyncPayload> job) {
        // 1. Thread-local cursor automatically injected by CheckpointStatePlugin
        String currentToken = CheckpointStatePlugin.getCurrentCheckpoint();
        
        // 2. Fetch changes from cloud API starting from cursor
        DeltaResult page = cloudClient.getDeltaChanges(job.getData().driveId(), currentToken);
        for (FileItem item : page.items()) {
            enqueueFileInspection(item);
        }

        // 3. Return updated payload with new delta token -> automatically committed to Redis!
        return new DriveSyncPayload(
                job.getData().tenantId(),
                job.getData().driveId(),
                page.nextDeltaToken()
        );
    }
}
```

---

## 6. Chained Enterprise Resilience Composition

All five OxMQ plugins can be chained seamlessly on the same worker pipeline, forming an enterprise-grade defense-in-depth architecture:

```mermaid
flowchart LR
    A[Worker Fetch] --> B[1. CheckpointStatePlugin<br/>Inject Cursor]
    B --> C[2. RateLimitSentinelPlugin<br/>Check 429 Cooldown]
    C --> D[3. PriorityQuotaGovernorPlugin<br/>Yield if Low Headroom]
    D --> E[4. GroupRateLimiterPlugin<br/>Acquire Token Bucket]
    E --> F[Execute Business Task]
    F --> G[5. AdaptiveRateLimiterPlugin<br/>AIMD Feedback]
    F --> H[Checkpoint Commit]
```

---

## 🛠️ Building Custom Interceptors

You can implement the [`JobInterceptor`](file:///Users/gaurav/git/oxmq/oxmq-core/src/main/java/io/oxmq/interceptor/JobInterceptor.java) interface to add cross-cutting concerns to your workers.

### Example: Distributed Tracing & MDC Propagation Interceptor

This interceptor extracts a trace ID from job metadata, binds it to SLF4J MDC before execution so all logs include the trace context, and cleanly purges MDC after completion:

```java
import io.oxmq.interceptor.ErrorDecision;
import io.oxmq.interceptor.InterceptorDecision;
import io.oxmq.interceptor.JobInterceptor;
import io.oxmq.model.Job;
import org.slf4j.MDC;
import org.springframework.stereotype.Component;
import java.time.Duration;

@Component
public class MdcTracingInterceptor implements JobInterceptor {

    private static final String MDC_TRACE_ID = "traceId";
    private static final String MDC_JOB_ID = "jobId";

    @Override
    public <T> InterceptorDecision beforeProcess(Job<T> job) {
        String traceId = job.getOpts() != null && job.getOpts().getJobId() != null
                ? job.getOpts().getJobId()
                : job.getId();

        MDC.put(MDC_TRACE_ID, traceId);
        MDC.put(MDC_JOB_ID, job.getId());

        return InterceptorDecision.proceed();
    }

    @Override
    public <T> void afterProcess(Job<T> job, Duration executionDuration) {
        MDC.remove(MDC_TRACE_ID);
        MDC.remove(MDC_JOB_ID);
    }

    @Override
    public <T> ErrorDecision onError(Job<T> job, Throwable error, Duration duration) {
        MDC.remove(MDC_TRACE_ID);
        MDC.remove(MDC_JOB_ID);
        return ErrorDecision.bubbleUp(); // Delegate to standard BullMQ retry logic
    }
}
```

---

## 📊 Prometheus & Grafana Observability

All plugins natively export high-precision telemetry to Micrometer.

### Exported Metrics Catalog

| Metric Name | Type | Tags | Description |
| :--- | :--- | :--- | :--- |
| `oxmq.plugin.ratelimiter.yields` | Counter | `group`, `queue` | Total jobs delayed by dynamic token-bucket limit exhaustion. |
| `oxmq.plugin.sentinel.delays` | Counter | `group`, `queue` | Total jobs intercepted and delayed by active 429/503 sentinel cooldowns. |
| `oxmq.plugin.sentinel.cooldowns` | Counter | `group` | Total 429 circuit-breaker trips activated across the cluster. |
| `oxmq.plugin.governor.yields` | Counter | `group`, `queue` | Total low-priority jobs yielded to protect high-priority quota headroom. |

### Bundled Enterprise Grafana Dashboards

OxMQ ships with two pre-configured, production-ready Grafana dashboards:

#### 1. Queue Telemetry & Operations Dashboard (`oxmq-grafana-dashboard.json`)
Packaged inside `oxmq-spring-boot-starter/src/main/resources/dashboards/oxmq-grafana-dashboard.json`:
- **Queue Backlog & States**: Real-time gauges for Waiting, Active, Delayed, and Failed jobs.
- **Cluster Throughput**: Ingestion rate vs completion rate ($jobs/sec$) across all queues.
- **Multi-Tenant Token Bucket Yields**: Stacked time-series breakdown of yields per tenant.
- **Sentinel Circuit-Breaker Cooldowns**: Heatmap and alert state for external API throttling.
- **Execution Latency Percentiles**: $p50$, $p95$, and $p99$ execution durations per queue.

#### 2. System Metrics & Resource Isolation Dashboard (`oxmq-system-metrics.json`)
Packaged inside `oxmq-examples/oxdoc-365/docker/grafana/dashboards/oxmq-system-metrics.json`:
- **Isolated JVM Heap Memory Pools**: Real-time memory consumption broken down by **Eden Space**, **Survivor Space**, and **Old Gen / Tenured Space** (verified peak usage: $< 110\text{ MB}$ under 5,000-job load).
- **Non-Heap & Metaspace**: Classloader and Metaspace memory tracking to ensure zero memory leaks.
- **Process CPU & Worker Concurrency**: Host and process CPU utilization paired with active worker thread counts.
- **Database Connection Pools (HikariCP)**: Active, idle, and pending database connections.
- **Dynamic Adaptive Capacity Gauges**: Real-time gauges tracking adaptive capacity limits per tenant/group key adjusted by `AdaptiveRateLimiterPlugin`.

Import these JSON templates into Grafana (`Dashboards -> New -> Import`) with a Prometheus datasource to monitor your production queues and system resources immediately.
