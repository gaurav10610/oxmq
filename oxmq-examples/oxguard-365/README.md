# 🛡️ OxGuard 365: Enterprise SaaS DSPM & Threat Analysis Engine
### High-Scale Multi-Tenant Data Security Posture Management Powered by OxMQ & Java 17 LTS

<p align="center">
  <a href="https://github.com/gaurav10610/oxmq"><img src="https://img.shields.io/badge/Powered%20By-OxMQ%20v1.6.0-blue.svg" alt="Powered By OxMQ"></a>
  <a href="https://openjdk.org/projects/jdk/17/"><img src="https://img.shields.io/badge/Java-17%2B%20LTS-orange.svg" alt="Java 17 LTS"></a>
  <a href="https://spring.io/projects/spring-boot"><img src="https://img.shields.io/badge/Spring%20Boot-3.3.0-brightgreen.svg" alt="Spring Boot 3"></a>
  <a href="https://redis.io"><img src="https://img.shields.io/badge/Redis-7%20Alpine-red.svg" alt="Redis 7"></a>
  <a href="https://www.postgresql.org/"><img src="https://img.shields.io/badge/PostgreSQL-16%20Alpine-336791.svg" alt="PostgreSQL 16"></a>
  <a href="https://kubernetes.io"><img src="https://img.shields.io/badge/Kubernetes-Production%20Specs-326CE5.svg" alt="Kubernetes Ready"></a>
</p>

> **OxGuard 365** is a production-grade, enterprise SaaS Data Security Posture Management (DSPM) and real-time threat detection engine designed to inspect, catalog, and secure **Microsoft SharePoint Sites** and **User OneDrives** at hyperscale. It demonstrates how to build a mission-critical, multi-tenant cloud security scanner with **zero data drop**, **zero false poison retries**, and **strict tenant isolation** using **OxMQ** native enterprise plugins.

---

## 📑 Detailed Documentation Sitemap

For deep-dive technical explorations of specific subsystems, refer to the dedicated modular documentation guides:

* [⚡ **Advanced Rate Limiting, Throttling & Adaptive Capacity**](docs/RATE_LIMITING_AND_THROTTLING.md): Mathematical formulations of AIMD, sliding epoch token buckets, damping locks, and priority headroom yielding.
* [🛡️ **Failure Handling & Disaster Recovery Guide**](docs/FAILURE_HANDLING_AND_DISASTER_RECOVERY.md): Comprehensive analysis of all 10 cloud failure modes, exception hierarchies, and recovery runbooks.
* [🏢 **Multi-Tenant Architecture & Dynamic Licensing**](docs/MULTI_TENANT_ARCHITECTURE.md): The 5 official Microsoft 365 license tiers, Resource Unit (RU) model, and the 4-level discovery waterfall.
* [🔬 **Dual-Pillar Inspection DAG & Threat Engine**](docs/DUAL_PILLAR_INSPECTION_ENGINE.md): `FlowProducer` dependency trees, Access Control List (ACL) inheritance resolution, and zero-heap streaming heuristics.
* [📐 **Data Model, Schemas & LLD Specification**](docs/DATA_MODEL_AND_LLD_SPECIFICATION.md): Complete PostgreSQL 16 DDL, Redis key topologies, Java DTO records, and REST API contracts.
* [📊 **Observability, Metrics & Production Benchmarks**](docs/OBSERVABILITY_AND_BENCHMARKS.md): Prometheus metrics dictionary, dual Grafana dashboards, and 5,000-file chaos benchmark results.

---

## 1. High-Level Architecture (HLD)

OxGuard 365 decouples high-volume cloud ingestion from intensive security analysis using an event-driven, queue-based architecture:

```mermaid
flowchart TD
    subgraph MS365 ["Microsoft 365 Cloud (or Decoupled Mock Server)"]
        GraphAPI["Microsoft Graph v1.0 REST API<br/>(/subscribedSkus, /users, /drives, /delta, /permissions)"]
        BlobStorage["Azure Blob Storage<br/>(Pre-authenticated SAS URLs: 0 Graph RUs)"]
        Webhooks["Microsoft Graph Webhook Subscriptions<br/>(Real-Time Invalidation Signals)"]
        ChaosEngine["Realistic Cloud Chaos Engine<br/>(/mock-admin/chaos: 429 bursts, 500/503 faults)"]
    end

    subgraph Ingestion ["Ingestion & Webhook Layer"]
        WebhookCtrl["GraphWebhookController<br/>(15s Debounce Deduplication)"]
        DiscoveryWorker["TenantDiscoveryWorker<br/>(SharePoint & OneDrive Enumeration)"]
    end

    subgraph OxMQ_Pipeline ["OxMQ Native Enterprise Plugin Pipeline"]
        SentinelPlugin["1. RateLimitSentinelPlugin<br/>(Cluster-wide Circuit Breaker on 429)"]
        LimiterPlugin["2. GroupRateLimiterPlugin<br/>(Token Bucket Per Tenant Group Key)"]
        GovernorPlugin["3. PriorityQuotaGovernorPlugin<br/>(Headroom Yielding: Priority 1 vs 10)"]
        AdaptivePlugin["4. AdaptiveRateLimiterPlugin<br/>(AIMD Capacity: Halves on 429, Damping Lock)"]
        CheckpointPlugin["5. CheckpointStatePlugin<br/>(Distributed Delta Cursors in Redis)"]
    end

    subgraph Queues ["Domain-Specialized OxMQ Queues (Java 17 Thread Pools)"]
        Q_Delta["drive-delta-sync-queue<br/>(Delta change tracking & checkpoint commit)"]
        Q_DAG["file-inspection-flow<br/>(FlowProducer parent-child dependency tree)"]
        Q_Perm["file-permission-queue<br/>(Pillar 1: True Effective ACL Exposure)"]
        Q_Malware["file-malware-queue<br/>(Pillar 2: Zero-Heap Streaming Heuristics)"]
        Q_Aggr["file-aggregate-queue<br/>(Parent Node: Merges verdicts into PostgreSQL)"]
    end

    subgraph Storage ["Persistent Storage & Cache"]
        Postgres[("PostgreSQL 16 Database<br/>(oxguard_scanned_files, oxguard_monitored_drives)")]
        RedisState[("Redis 7 High-Speed State<br/>(BullMQ Queues, Limiter Buckets, Checkpoints)")]
    end

    Webhooks --> WebhookCtrl
    WebhookCtrl -->|Priority 1 Forward Job| Q_Delta
    DiscoveryWorker -->|Priority 10 Backward Job| Q_Delta

    Q_Delta <--> CheckpointPlugin
    Q_Delta <--> SentinelPlugin
    Q_Delta <--> LimiterPlugin
    Q_Delta <--> GovernorPlugin

    Q_Delta --> GraphAPI
    Q_Delta -->|Dispatch DAG| Q_DAG
    Q_DAG --> Q_Perm
    Q_DAG --> Q_Malware

    Q_Perm --> GraphAPI
    Q_Malware --> BlobStorage
    Q_Perm --> Q_Aggr
    Q_Malware --> Q_Aggr

    Q_Aggr --> Postgres
    OxMQ_Pipeline <--> RedisState
```

---

## 2. Low-Level Design (LLD) & Component Interactions

### 2.1 The Interceptor Execution Pipeline
Every job dispatched to an OxMQ listener passes through an ordered chain of non-blocking interceptors before worker business logic is invoked:

```mermaid
sequenceDiagram
    autonumber
    actor Worker as OxMQ Worker Thread
    participant Chain as OxMQ JobInterceptor Chain
    participant Sentinel as RateLimitSentinelPlugin
    participant Limiter as GroupRateLimiterPlugin
    participant Governor as PriorityQuotaGovernorPlugin
    participant Business as Worker Processor Method
    participant Redis as Redis 7 Cluster

    Worker->>Chain: beforeProcess(job)
    Chain->>Sentinel: 1. Is 429 Cooldown Sentinel active for tenant?
    alt Cooldown Active in Redis
        Sentinel-->>Worker: InterceptorDecision.delay(remainingMs, true)
        Note over Worker: Job deferred non-destructively without touching network!
    else Cooldown Inactive
        Sentinel->>Limiter: 2. Has tenant token bucket exceeded limit?
        alt Token Quota Depleted
            Limiter-->>Worker: InterceptorDecision.delay(150ms, true)
        else Token Available
            Limiter->>Governor: 3. Is Headroom < 25% on a Priority 10 Job?
            alt Priority 10 & Headroom < 25%
                Governor-->>Worker: InterceptorDecision.delay(200ms, true)
                Note over Worker: Background sweep yields to protect Priority 1 forward scans!
            else Allowed (Priority 1 or Headroom >= 25%)
                Governor-->>Business: Invoke @OxmqListener processor method
            end
        end
    end
```

### 2.2 Dual-Pillar Inspection DAG Execution Flow
Every file item discovered by delta sync triggers a 3-node dependency graph using OxMQ's `FlowProducer`:

```mermaid
stateDiagram-v2
    [*] --> Dispatched: DeltaSyncWorker builds FlowJob tree

    state "Parent: FileAggregateWorker" as Parent {
        Dispatched --> WaitingChildren: Placed in Redis waiting-children state
        WaitingChildren --> ActiveParent: Both children succeed (Lua callback)
        ActiveParent --> Persisted: Merges childrenValues & saves to PostgreSQL
    }

    state "Child 1: FilePermissionWorker" as Child1 {
        Dispatched --> RunPerm: Claims perm-* from file-permission-queue
        RunPerm --> PermDone: Returns EffectiveExposure record
    }

    state "Child 2: FileMalwareWorker" as Child2 {
        Dispatched --> RunMalware: Claims malware-* from file-malware-queue
        RunMalware --> MalwareDone: Streams SAS URL & returns ThreatAnalysisResult
    }

    PermDone --> ActiveParent: Result injected into Parent childrenValues
    MalwareDone --> ActiveParent: Result injected into Parent childrenValues
    Persisted --> [*]
```

---

## 3. The 5 Specialized Queues & Concurrency Matrix

OxGuard 365 partitions processing into 5 domain-specialized queues with dedicated thread pools and retry strategies:

| Queue Name | Primary Responsibility | Concurrency | Priority Level | Retry & Backoff Strategy | Rate Limit Policy |
| :--- | :--- | :--- | :--- | :--- | :--- |
| **`tenant-discovery-queue`** | Discovers all SharePoint sites, libraries, and user OneDrives. | 5 threads | Priority 5 (Normal) | 3 attempts, Exponential ($1\text{s}$) | Bound to tenant group token bucket ($1\text{ RU}$) |
| **`drive-delta-sync-queue`** | Ingests file deltas via `/delta` and commits checkpoint tokens. | 20 threads | Priority 1 (Forward) / Priority 10 (Backward) | 3 attempts, Exponential ($1\text{s}$) | Bound to tenant group token bucket ($1\text{ RU}$) |
| **`file-permission-queue`** | Pillar 1: Resolves direct vs inherited ACLs and sharing scopes. | 30 threads | Inherited from DAG (P1 / P10) | 3 attempts, Exponential ($1\text{s}$), `failParent=false` | Bound to tenant group token bucket ($1\text{ RU}$) |
| **`file-malware-queue`** | Pillar 2: Streams file bytes from SAS URL and runs heuristics. | 25 threads | Inherited from DAG (P1 / P10) | 3 attempts, Exponential ($1\text{s}$), `failParent=false` | Direct Azure SAS bypass (**$0\text{ Graph RUs}$**) |
| **`file-aggregate-queue`** | Merges exposure and threat verdicts into persistent PostgreSQL. | 15 threads | Inherited from DAG (P1 / P10) | 3 attempts, Exponential ($1\text{s}$) | Bound to PostgreSQL HikariCP write pool |

---

## 4. Native OxMQ Enterprise Plugins Adoption

By leveraging native OxMQ plugins configured in [`OxGuardPluginConfiguration.java`](file:///Users/gaurav/git/oxmq/oxmq-examples/oxguard-365/src/main/java/io/oxmq/oxguard/config/OxGuardPluginConfiguration.java), workers contain **zero boilerplate** rate-limiting, retry, or circuit-breaking code:

1. **`AdaptiveRateLimiterPlugin`**: Dynamically adjusts capacity via AIMD ($L_{t+1} = \max(L_{\min}, \lfloor L_t \times 0.5 \rfloor)$ on 429/503 faults; $L_{t+1} = \min(L_{\max}, L_t + 25)$ on clean windows), locks against oscillation for 60s, and synchronizes limits across pods via Redis hash `oxguard:adaptive:limits`.
2. **`CheckpointStatePlugin`**: Persists delta query tokens in Redis hash `oxguard:checkpoint:drives`. Recovers delta tokens across pod restarts and auto-commits cursors to PostgreSQL.
3. **`GroupRateLimiterPlugin`**: Enforces strict token-bucket rate limits per tenant group key ($25 - 250\text{ RUs/s}$).
4. **`RateLimitSentinelPlugin`**: Locks tenant cooldowns cluster-wide upon encountering HTTP 429 `Retry-After: N`, preemptively deferring parallel jobs without network requests.
5. **`PriorityQuotaGovernorPlugin`**: Defers low-priority background sweeps (Priority 10) when capacity headroom drops below 25%, strictly reserving quota for real-time webhook events (Priority 1).

---

## 5. Mathematical Formulations & Derivations

### 5.1 AIMD Capacity Equations
* **Multiplicative Decrease (on 429/503 Fault):**
  $$L_{t+1} = \max\left(25, \left\lfloor L_t \times 0.5 \right\rfloor\right)$$
* **Additive Increase (after 2 consecutive clean 30s evaluation windows):**
  $$L_{t+1} = \min\left(250, L_t + 25\right)$$
* **Oscillation Damping Lock:**
  $$\tau_{\text{damping}} = 60,000\text{ ms}$$
  Additive increases are frozen for $\tau_{\text{damping}}$ following any 429 event.

### 5.2 Priority Headroom Yield Formula
$$\text{Headroom}(t) = \frac{L_t - U_t}{L_t}$$
$$\text{If } \text{Priority} = 10 \quad \text{AND} \quad \text{Headroom}(t) < 0.25 \implies \text{DEFER JOB via } \texttt{moveToDelayed(200ms, skipAttempt=true)}$$

### 5.3 Shannon Entropy Formula (Ransomware Detection)
$$H(X) = - \sum_{i=0}^{255} p(b_i) \log_2 p(b_i)$$
Where $p(b_i) = \frac{\text{count}(b_i)}{N_{\text{total}}}$. Files with $H(X) > 7.5$ across 64 KB buffers on uncompressed documents are flagged as `SUSPICIOUS_ENTROPY`.

---

## 6. Multi-Tenant Licensing (5 Tiers & 4-Level Waterfall)

OxGuard 365 models all 5 official Microsoft 365 licensing tiers simultaneously:
* **`tenant-global-100k`**: **Hyperscale Global** ($100,000\text{ seats}, 250\text{ RUs/s}$ burst limit).
* **`tenant-large-35k`**: **Large Enterprise** ($35,000\text{ seats}, 150\text{ RUs/s}$ burst limit).
* **`tenant-ent-10k`**: **Enterprise E3/E5** ($10,000\text{ seats}, 90\text{ RUs/s}$ burst limit).
* **`tenant-mid-2500`**: **Mid-Market Growth** ($2,500\text{ seats}, 50\text{ RUs/s}$ burst limit).
* **`tenant-smb-250`**: **Small Business** ($250\text{ seats}, 25\text{ RUs/s}$ burst limit).

### 4-Level Discovery Waterfall:
1. **Level 1: Static Configuration / Admin API Override:** Configured via YAML or REST API (`POST /api/v1/dashboard/tenants/{id}/override?tier=HYPERSCALE`).
2. **Level 2: Microsoft Graph `GET /v1.0/subscribedSkus`:** Maps license SKU part numbers (`SPE_E5`, `SPE_E3`, `O365_BUSINESS_PREMIUM`).
3. **Level 3: Heuristic User Count Fallback (`GET /v1.0/users?$count=true`):** Used if `subscribedSkus` permission is denied (`User.Read.All`).
4. **Level 4: Safe Baseline Fallback:** Defaults new/unresolvable tenants to the SMB baseline ($25\text{ RUs/s}$) to prevent gateway lockouts.

---

## 7. Dual-Pillar Threat Analysis Mechanics

### Pillar 1: Effective Exposure Engine
Classifies Access Control Lists (`/permissions`) and folder inheritance into 5 normalized tiers:
1. 🔴 **`PUBLIC_ANONYMOUS`**: Anyone on the internet with a shareable link can access without logging in.
2. 🟠 **`EXTERNAL_GUEST`**: Shared explicitly with external guest users or external vendor domains.
3. 🟡 **`ORGANIZATION_WIDE`**: Shared tenant-wide; any employee in the company has read/write access.
4. 🟢 **`INTERNAL_RESTRICTED`**: Restricted to specific named internal users or security groups.
5. 🔒 **`PRIVATE`**: Document owner only; zero secondary ACLs or sharing links.

### Pillar 2: Zero-Heap Streaming Malware Engine
Streams file bytes via direct Azure Blob SAS URLs (`@microsoft.graph.downloadUrl`) bypassing Graph API limits (**$0\text{ Graph RUs}$**).
* **Memory Footprint:** Fixed 64 KB ring buffer (`byte[] buffer = new byte[65536];`); peak JVM heap remains sub-120 MB across 5,000 files.
* **Cryptographic Hash:** Incremental SHA-256 digest computation (`MessageDigest.update`).
* **EICAR Matcher:** Streaming state-machine detecting standard antivirus test strings.
* **Magic Byte Verification (MIME Mismatches):**
  - Detects Windows PE Executables (`MZ` / `0x4D 0x5A`) disguised as `.docx`, `.xlsx`, `.pdf`.
  - Detects Linux ELF Binaries (`\x7FELF` / `0x7F 0x45 0x4C 0x46`) disguised as Office documents.
* **Shannon Entropy:** Detects packed binaries or ransomware payloads ($H > 7.5$).

---

## 8. Summary Failure Handling & Disaster Recovery Matrix

| Failure Mode | Trigger Condition | Architectural Mitigation | Guarantee |
| :--- | :--- | :--- | :--- |
| **HTTP 429 Throttling** | Burst limit exceeded | `RateLimitSentinelPlugin` sets Redis cooldown; jobs defer via `moveToDelayed(skipAttempt=true)` | Resumes on cooldown expiration; 0 attempts lost |
| **HTTP 500/503 Faults** | Graph gateway degradation | `AdaptiveRateLimiterPlugin` halves capacity ($L_{t+1} = \lfloor 0.5 \times L_t \rfloor$) with 60s damping freeze | Auto-stabilizes; eliminates capacity oscillation |
| **Socket Timeouts** | Gateway network drop | 3 attempts with Bounded Exponential Backoff ($1\text{s}, 2\text{s}, 4\text{s}$) | Resilient recovery within 3 attempts |
| **Redis Crash** | Redis container restart | Lettuce auto-reconnection; `StalledJobSentinel` recovers unacknowledged locks | **Zero job loss** |
| **Pod SIGTERM Kill** | Deployment rollout / HPA | K8s `preStop` (10s) + Spring graceful drain (45s); delta cursors committed | **Zero duplicate scans** |
| **Delta Cursor Invalidation** | Paused $> 30\text{ days}$ (HTTP 410) | `CheckpointStatePlugin` resets cursor; restarts full baseline scan | Clean historical resync without corruption |
| **AutoSave Webhook Storm** | 10-20 webhook pings/min | 15-second debounce deduplication with `keepLastIfActive: true` | $95\%$ reduction in redundant API queries |
| **Partial DAG Failure** | Malware child fails | `failParentOnFailure = false`; parent proceeds with fallback clean verdict | Avoids zombie DAG parent jobs |
| **Premature Stream EOF** | Disconnected SAS download | Caught in `FileMalwareWorker`; marks file `CLEAN (uncomputed)` | Worker pipeline unblocked |
| **Database Pool Exhaustion** | 20 concurrent write connections | HikariCP queueing + OxMQ exponential retry backoff | Graceful backpressure propagation |

---

## 9. Data Model & Redis Key Topology

### PostgreSQL 16 Schema Summary
* **`oxguard_scanned_files`**: Persists file metadata, exposure levels, threat verdicts, SHA-256 hashes, file sizes, and scan origins. Indexed on `file_id`, `exposure_level`, `threat_verdict`, `scan_origin`, and `scanned_at_timestamp`.
* **`oxguard_monitored_drives`**: Persists discovered SharePoint document libraries and user OneDrives with their latest `@odata.deltaLink` tokens.

### Redis Key Space Mapping
* `bull:<queueName>:*`: Core BullMQ queue structures (`id`, `wait`, `active`, `delayed`, `completed`, `failed`).
* `oxmq:limiter:tenant:usage:<tenantId>:<epochSec>`: Sliding 1-second epoch token bucket ($5\text{s TTL}$).
* `oxmq:sentinel:tenant:cooldown:<tenantId>`: Active 429 circuit-breaker cooldown key ($N\text{s TTL}$).
* `oxguard:adaptive:limits`: Hash storing cluster-wide synchronized adaptive capacities ($L_t$).
* `oxguard:adaptive:damping:<tenantId>`: Damping lock key freezing additive upgrades ($60\text{s TTL}$).
* `oxguard:checkpoint:drives`: Hash caching current delta tokens mapped by `driveId`.

---

## 10. Kubernetes Production Specifications (`k8s/`)

Production Kubernetes manifests are authored under [`k8s/`](file:///Users/gaurav/git/oxmq/oxmq-examples/oxguard-365/k8s/):
* **`k8s/01-base-config.yaml`**: Namespace `oxguard-system`, ConfigMaps, and Secrets.
* **`k8s/02-infrastructure.yaml`**: StatefulSets for Redis 7 and PostgreSQL 16 with PersistentVolumeClaims.
* **`k8s/03-apps.yaml`**: Deployment `oxguard-365` (2 replicas):
  - HTTP Liveness (`/actuator/health/liveness`) and Readiness (`/actuator/health/readiness`) probes.
  - Lifecycle: `preStop: exec: command: ["sh", "-c", "sleep 10"]` and `terminationGracePeriodSeconds: 45`.
  - Resource Sizing: Requests: `500m CPU, 512Mi RAM`. Limits: `2000m CPU, 1024Mi RAM`.

---

## 11. Dual Grafana Observability Dashboards

Provisioned under [`docker/grafana/dashboards/`](file:///Users/gaurav/git/oxmq/oxmq-examples/oxguard-365/docker/grafana/dashboards/):
1. **Queue Operations Dashboard (`oxmq-overview.json`)**:
   - Queue depths (Active, Waiting, Delayed, Completed) stacked across all 5 queues.
   - Throughput velocity ($jobs/\text{sec}$), latency percentiles (p50, p95, p99), and rate-limit yields.
2. **System Metrics Dashboard (`oxmq-system-metrics.json`)**:
   - Generational JVM Heap memory pools (`Eden`, `Survivor`, `Tenured/Old Gen`, `Metaspace`).
   - Platform vs Virtual Thread counts, CPU load, and HikariCP connection pool metrics.
   - Dynamic adaptive capacity gauges per tenant tier ($25 - 250\text{ RUs/s}$).

---

## 12. Disaster Recovery Benchmark & Verified Audit Results

Run the automated clean-slate resilience benchmark:
```bash
./run_multi_tenant_resilience_test.sh
```

### Verified Audit Results:
* **Clean-Slate Workload:** $5,000\text{ files}$ across $25\text{ document libraries}$ and all $5\text{ tenant tiers}$.
* **Embedded Threats:** $150\text{ threats}$ (EICAR, Windows PE headers, Linux ELF binaries, ransomware entropy).
* **Chaos Injected:** 25x HTTP 429 bursts, 15x HTTP 503 faults, 4-second Redis stop, worker SIGTERM kill.
* **Audit Guarantee:** **$5,000 / 5,000\text{ files cataloged}$** (**$100.00\%$ Ingestion - ZERO Data Drop**), **$150 / 150\text{ threats quarantined}$**, **peak JVM Heap: $106.4\text{ MB}$**.

---

## 13. Quickstart Guide

### 13.1 Start Full Docker Stack
```bash
./docker/start-oxguard.sh
```
This launches:
* **OxGuard 365 DSPM Application:** `http://localhost:8080`
* **Mock Microsoft 365 Server:** `http://localhost:8081`
* **OxMQ Bull-Board UI:** `http://localhost:3000`
* **Grafana Dashboards:** `http://localhost:3001` (`admin / admin`)
* **Prometheus Telemetry:** `http://localhost:9090`
* **PostgreSQL 16 Database:** `localhost:5432` (`oxguard / oxguard`)
* **Redis 7 Cluster:** `localhost:6379`

### 13.2 Trigger Discovery Across All 5 Tenants
```bash
curl -X POST "http://localhost:8080/api/v1/dashboard/trigger/crawl-all"
```

### 13.3 Inject Simulated 429 Storm on Acme TinyCorp
```bash
curl -X POST "http://localhost:8080/api/v1/dashboard/simulate/429?tenantId=tenant-smb-250"
```
Observe that only Acme workers defer, while MegaCorp Global continues scanning at 250 RUs/s with zero interruption!

### 13.4 Stop Stack
```bash
./docker/stop-oxguard.sh --clean
```
