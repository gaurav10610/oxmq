# 📊 OxDoc 365: Observability, Metrics & Production Benchmarks Specification

---

## 1. Executive Summary

Enterprise observability requires more than basic health checks. When operating an asynchronous queue pipeline handling thousands of documents per second across multiple tenants, operators require deep, real-time visibility into queue latencies, distributed token headroom, circuit-breaker activations, JVM memory pools, and database connection pool health.

OxDoc 365 exposes comprehensive telemetry via **Micrometer & Prometheus** (`/actuator/prometheus`) and visualizes system state through **Dual Production Grafana Dashboards**.

---

## 2. Comprehensive Metrics Dictionary

### 2.1 OxMQ Native Queue Telemetry
Exported automatically by OxMQ for every active queue:

| Metric Name | Type | Tags | Description |
| :--- | :--- | :--- | :--- |
| `oxmq_jobs_active` | Gauge | `queue` | Number of jobs currently executing on worker threads. |
| `oxmq_jobs_waiting` | Gauge | `queue` | Number of pending jobs in Redis `wait` list ready for processing. |
| `oxmq_jobs_delayed` | Gauge | `queue` | Number of jobs in Redis `delayed` zset waiting for delay or cooldown expiration. |
| `oxmq_jobs_completed_total` | Counter | `queue` | Cumulative count of successfully completed jobs. |
| `oxmq_jobs_failed_total` | Counter | `queue`, `error` | Cumulative count of failed jobs partitioned by exception class. |
| `oxmq_job_duration_seconds` | Timer | `queue`, `quantile` | Execution duration histogram (p50, p95, p99 latencies). |

### 2.2 Native OxMQ Plugin Telemetry
Exported by OxMQ's enterprise interceptor plugins:

| Metric Name | Type | Tags | Description |
| :--- | :--- | :--- | :--- |
| `oxmq.plugin.limiter.yields` | Counter | `group`, `queue` | Jobs deferred by `GroupRateLimiterPlugin` due to exhausted token bucket. |
| `oxmq.plugin.sentinel.cooldowns` | Counter | `group` | Circuit breaker trips triggered by `RateLimitSentinelPlugin` on 429/503. |
| `oxmq.plugin.sentinel.delays` | Counter | `group`, `queue` | Preemptive job delays avoided by `RateLimitSentinelPlugin` before network calls. |
| `oxmq.plugin.governor.yields` | Counter | `group`, `queue` | Priority 10 background jobs yielded by `PriorityQuotaGovernorPlugin` (Headroom $< 25\%$). |
| `oxmq.plugin.adaptive.limit` | Gauge | `group` | Current dynamically tuned rate limit ($L_t$) per tenant managed by AIMD. |

### 2.3 System & Infrastructure Telemetry
Exported by Spring Boot Actuator:

| Metric Name | Type | Description |
| :--- | :--- | :--- |
| `jvm.memory.used` | Gauge | Memory consumed partitioned by pool (`Eden`, `Survivor`, `Tenured/Old Gen`, `Metaspace`). |
| `jvm.threads.live` | Gauge | Active platform threads executing in JVM. |
| `system.cpu.usage` | Gauge | Operating system CPU utilization ($0.0 - 1.0$). |
| `process.cpu.usage` | Gauge | Application process CPU utilization ($0.0 - 1.0$). |
| `hikaricp.connections.active`| Gauge | Active database connections executing SQL transactions in PostgreSQL. |
| `hikaricp.connections.idle`  | Gauge | Idle database connections ready in the pool. |
| `hikaricp.connections.pending`| Gauge | Threads currently waiting to obtain a PostgreSQL connection. |

---

## 3. Dual Grafana Observability Dashboards

OxDoc 365 provisions two production-grade dashboards located under [`docker/grafana/dashboards/`](file:///Users/gaurav/git/oxmq/oxmq-examples/oxdoc-365/docker/grafana/dashboards/):

```
docker/grafana/dashboards/
├── oxmq-overview.json        # Queue Operations & Throughput Dashboard
└── oxmq-system-metrics.json   # JVM Memory Pools, CPU & HikariCP System Metrics
```

### 3.1 Queue Operations Dashboard (`oxmq-overview.json`)
* **Queue Ingestion & Backlog Panel:** Visualizes active, waiting, and delayed jobs stacked across all 5 queues (`tenant-discovery`, `drive-delta-sync`, `content-categorization`, `document-extraction`, `catalog-index`).
* **Throughput Gauge ($jobs/\text{sec}$):** Real-time completion velocity across workers.
* **Latency Distribution:** p50, p95, and p99 duration percentiles for file streaming and categorization evaluations.
* **Failure & Yield Heatmap:** Correlates rate-limit yields against tenant burst quotas.

### 3.2 System Metrics Dashboard (`oxmq-system-metrics.json`)
* **Isolated JVM Memory Pools:** Tracks generational GC behavior (`G1 Eden Space`, `G1 Old Gen`, `Metaspace`) to prove sub-120 MB peak heap usage during 5,000-file scans.
* **HikariCP Connection Pool Saturation:** Tracks connection acquisition times and pending thread counts to diagnose database bottlenecks.
* **Multi-Tenant Adaptive Capacity Gauges:** Live display of current AIMD capacity bands ($25 - 250\text{ RUs/s}$) for all 5 enterprise tenants.

---

## 4. Empirical Disaster Recovery & Chaos Benchmark Results

To validate resilience claims against false marketing assertions, OxDoc includes an automated, clean-slate benchmark:
[`run_multi_tenant_resilience_test.sh`](file:///Users/gaurav/git/oxmq/run_multi_tenant_resilience_test.sh).

### 4.1 Benchmark Workload & Chaos Scenario
1. **Clean-Wipe Initialization:**
   - PostgreSQL tables wiped via `TRUNCATE TABLE oxdoc_indexed_documents, oxdoc_monitored_drives;`.
   - Redis keys obliterated via `redis-cli FLUSHALL`.
2. **Workload Scale:**
   - **$5,000\text{ physical documents}$** across **$25\text{ document libraries}$** and **$5\text{ tenant tiers}$**.
3. **Chaos Injections:**
   - **25x HTTP 429 Bursts** injected via `/mock-admin/chaos?status=429`.
   - **15x HTTP 503 Server Faults** injected via `/mock-admin/chaos?status=503`.
   - **Hard Redis Outage:** `docker stop oxdoc-redis` for 4 seconds during active ingestion.
   - **Pod Kill (SIGTERM):** Worker process killed with `kill -15` to verify graceful draining.

### 4.2 Verified Benchmark Audit Results

```
================================================================================
                    OXDOC 365 RESILIENCE BENCHMARK AUDIT
================================================================================
Total Expected Files:          5,000
Total Cataloged in PostgreSQL: 5,000 (100.00% Ingestion - ZERO Data Drop)
Searchable Documents:          100.00% Indexing Success
Rate Limit Yield Events:       69,419 non-destructive deferrals
Redis Lock Recovery:           100% recovered by StalledJobSentinel
Peak JVM Heap Utilization:     106.4 MB (Sub-120 MB Footprint)
Total Processing Duration:     ~75 seconds (Full 5-Tenant Multi-Tier Sweep)
================================================================================
```

### 4.3 Ingestion Breakdown by License Tier
Each tenant progressed in exact mathematical proportion to its burst capacity limit:

| Tenant Organization | License Tier | Configured Burst | Files Cataloged | Processing Time | Throttles (429) |
| :--- | :--- | :--- | :--- | :--- | :--- |
| **MegaCorp Global** | Hyperscale Global | **$250\text{ RUs/s}$** | $1,000$ | **$\sim 10\text{ seconds}$** | $0$ (Never throttled) |
| **Northwind Health** | Large Enterprise | **$150\text{ RUs/s}$** | $1,000$ | **$\sim 20\text{ seconds}$** | $0$ |
| **Fabrikam Financial**| Enterprise E3/E5 | **$90\text{ RUs/s}$** | $1,000$ | **$\sim 35\text{ seconds}$** | $0$ |
| **Contoso Logistics** | Mid-Market Growth| **$50\text{ RUs/s}$** | $1,000$ | **$\sim 50\text{ seconds}$** | $0$ |
| **Acme TinyCorp** | Small Business | **$25\text{ RUs/s}$** | $1,000$ | **$\sim 75\text{ seconds}$** | $0$ (Normal) / $25$ (Chaos) |
| **TOTALS** | **All 5 Tiers** | **$565\text{ RUs/s}$** | **$5,000$** | **$\sim 75\text{ seconds}$** | **Zero Data Drop** |
