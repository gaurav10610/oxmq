# 🛡️ OxGuard 365: Enterprise SaaS DSPM & Threat Analysis Engine

<p align="center">
  <a href="https://github.com/gaurav10610/oxmq"><img src="https://img.shields.io/badge/Powered%20By-OxMQ%20v1.5.0-blue.svg" alt="Powered By OxMQ"></a>
  <a href="https://openjdk.org/projects/jdk/17/"><img src="https://img.shields.io/badge/Java-17%2B%20LTS-orange.svg" alt="Java 17 LTS"></a>
  <a href="https://spring.io/projects/spring-boot"><img src="https://img.shields.io/badge/Spring%20Boot-3.3.0-brightgreen.svg" alt="Spring Boot 3"></a>
  <a href="https://redis.io"><img src="https://img.shields.io/badge/Redis-7-red.svg" alt="Redis 7"></a>
  <a href="https://www.postgresql.org/"><img src="https://img.shields.io/badge/PostgreSQL-16-336791.svg" alt="PostgreSQL 16"></a>
</p>

> **OxGuard 365** is a production-grade SaaS Data Security Posture Management (DSPM) and real-time threat detection engine designed to inspect, catalog, and secure **SharePoint Sites** and **User OneDrives** at hyperscale, built on **Java 17 LTS** and **OxMQ**.

---

## 🌟 Key Architecture & Capabilities

```mermaid
flowchart TD
    subgraph MS365 ["Microsoft 365 Cloud (or Decoupled Mock Server)"]
        GraphAPI["Microsoft Graph v1.0 REST API<br/>(/subscribedSkus, /drives, /delta, /permissions)"]
        ChaosEngine["Realistic Cloud Chaos Engine<br/>(/mock-admin/chaos: 429 bursts, 500/503 faults)"]
    end

    subgraph OxMQ_Engine ["OxMQ Native Enterprise Plugins Pipeline"]
        AdaptivePlugin["AdaptiveRateLimiterPlugin<br/>(AIMD Capacity: Halves on 429, Damping Lock)"]
        CheckpointPlugin["CheckpointStatePlugin<br/>(Distributed Delta Cursors in Redis)"]
        SentinelPlugin["RateLimitSentinelPlugin<br/>(Cluster-wide Retry-After Circuit Breaker)"]
        GovernorPlugin["PriorityQuotaGovernorPlugin<br/>(Headroom Yielding: P1 VIP vs P10 Crawl)"]
        LimiterPlugin["GroupRateLimiterPlugin<br/>(Token Bucket Per Tenant Group Key)"]
    end

    subgraph Workers ["OxMQ Worker Pipeline (Java 17 Thread Pools)"]
        Q_Delta["drive-delta-sync-queue<br/>(Delta change tracking & checkpoint commit)"]
        Q_DAG["file-inspection-flow<br/>(FlowProducer parent-child tree)"]
        Q_Perm["file-permission-queue<br/>(Pillar 1: True Effective ACL Exposure)"]
        Q_Malware["file-malware-queue<br/>(Pillar 2: Zero-Heap Streaming Heuristics)"]
        Q_Aggr["file-aggregate-queue<br/>(Merges verdicts & updates PostgreSQL)"]
    end

    GraphAPI <--> AdaptivePlugin
    AdaptivePlugin --> LimiterPlugin
    CheckpointPlugin <--> Q_Delta
    SentinelPlugin --> Q_Delta
    GovernorPlugin --> Q_Delta
    Q_Delta --> Q_DAG
    Q_DAG --> Q_Perm
    Q_DAG --> Q_Malware
    Q_Perm --> Q_Aggr
    Q_Malware --> Q_Aggr
```

### 1. Dynamic Multi-Tenant Licensing (5 Microsoft 365 Tiers)
OxGuard 365 models all 5 official Microsoft 365 licensing tiers simultaneously without static hardcoded assumptions:
* **`tenant-apex-global`**: **Enterprise E5** (100,000 seats, 2,000 RUs/s burst capacity).
* **`tenant-northwind`**: **Enterprise E3** (35,000 seats, 1,200 RUs/s burst capacity).
* **`tenant-contoso`**: **Business Standard** (10,000 seats, 600 RUs/s burst capacity).
* **`tenant-fabrikam`**: **Business Basic** (2,500 seats, 300 RUs/s burst capacity).
* **`tenant-acme-smallbiz`**: **Free / Developer** (250 seats, 100 RUs/s burst capacity).

**4-Level Discovery Waterfall**:
1. *Static Override*: Configured via YAML (`oxguard.tenants.overrides.<tenantId>=<TIER>`) or REST Admin API.
2. *Graph API SKUs*: Discovers active SKUs (`O365_BUSINESS_PREMIUM`, `SPE_E3`, `SPE_E5`) via `GET /v1.0/subscribedSkus`.
3. *User Count Heuristic*: Queries `GET /v1.0/users?$count=true` using standard `User.Read.All` scope if `subscribedSkus` is denied.
4. *Safe Baseline Fallback*: Defaults new/unresolvable tenants to Free/Dev baseline.

### 2. Native OxMQ Plugins Adoption
OxGuard 365 leverages native plugins from `oxmq-core`, eliminating custom boilerplate while preserving 100% upstream BullMQ Lua compatibility:
- **`AdaptiveRateLimiterPlugin`**: Dynamically adjusts capacity via AIMD ($L_{t+1} = \max(L_{\min}, L_t \times 0.5)$ on 429/503 errors; $L_{t+1} = \min(L_{\max}, L_t + 25)$ on clean windows), locks against oscillation for 30s, and syncs cluster limits in Redis (`oxmq:adaptive:limits`).
- **`CheckpointStatePlugin`**: Persists delta query tokens in Redis hash `oxmq:checkpoint:m365-delta-sync`. Injects cursors via thread-local context and auto-commits tokens via `CheckpointSupplier`.
- **`GroupRateLimiterPlugin`**: Enforces token-bucket rate limits per tenant key.
- **`RateLimitSentinelPlugin`**: Locks tenant cooldowns cluster-wide upon encountering `Retry-After`.
- **`PriorityQuotaGovernorPlugin`**: Defers low-priority background sweeps (Priority 10) when capacity headroom drops below 25%, strictly reserving quota for real-time webhook events (Priority 1).

### 3. Dual-Pillar File Inspection DAG (`FlowProducer`)
- **Pillar 1: True Effective Exposure**: Inspects full Access Control Lists (`/permissions`), resolves folder inheritance (`inheritedFrom`), and classifies normalized exposure:
  🔴 `PUBLIC_ANONYMOUS`, 🟠 `EXTERNAL_GUEST`, 🟡 `ORGANIZATION_WIDE`, 🟢 `INTERNAL_RESTRICTED`, 🔒 `PRIVATE`.
- **Pillar 2: Zero-Heap Streaming Malware Engine**: Downloads file content via direct Azure Blob SAS URLs (`@microsoft.graph.downloadUrl`) bypassing Graph API limits. Streams bytes through 64 KB buffers computing:
  - SHA-256 cryptographic digest.
  - EICAR antivirus test signatures.
  - Shannon entropy ($H > 7.5$ flags potential ransomware).
  - MIME magic byte validation (detects PE/ELF executables disguised as Office docs).

---

## ☸️ Production Kubernetes Specs (`k8s/`)

Production Kubernetes manifests are authored in [`k8s/`](k8s/):
- **`k8s/01-base-config.yaml`**: Namespace `oxguard-system`, ConfigMaps, and Secrets.
- **`k8s/02-infrastructure.yaml`**: StatefulSets for Redis 7 and PostgreSQL 16 with PersistentVolumeClaims and health probes.
- **`k8s/03-apps.yaml`**:
  - `Deployment: oxguard-365` (2 replicas) with HTTP Liveness (`/actuator/health/liveness`) and Readiness (`/actuator/health/readiness`) probes.
  - Lifecycle Graceful Termination: `preStop: exec: command: ["sh", "-c", "sleep 10"]` and `terminationGracePeriodSeconds: 45`.
  - Resource Requests: 500m CPU, 512Mi RAM. Limits: 2000m CPU, 1024Mi RAM.

---

## 📊 Dual Grafana Observability Dashboards

1. **Queue Operations Dashboard (`oxmq-grafana-dashboard.json`)**:
   Tracks active, waiting, delayed, and failed queue depths, throughput ($jobs/sec$), p50/p95/p99 duration latencies, and tenant rate limit yields.
2. **System Metrics Dashboard (`oxmq-system-metrics.json`)**:
   Tracks isolated JVM Heap memory pools (Eden, Survivor, Tenured/Old Gen), Metaspace, process CPU, HikariCP database connection pool metrics, and dynamic adaptive capacity gauges per tenant tier.

---

## 🧪 Disaster Recovery & Cloud Chaos Benchmark

Run the automated clean-slate resilience benchmark:
```bash
./run_multi_tenant_resilience_test.sh
```

### Verified Resilience Results:
* **Clean-Wipe Workload**: 5,000 files and 150 embedded threats across all 5 tenant tiers.
* **Chaos Injected**: 25x HTTP 429 bursts and 15x HTTP 503 faults via `/mock-admin/chaos` (AIMD cut & 30s damping lock verified).
* **Redis Crash**: Stopped `oxguard-redis` for 4 seconds during active ingestion; Lettuce auto-reconnected and `StalledJobSentinel` recovered locks with **zero job loss**.
* **Pod Kill (SIGTERM)**: Sent `kill -15` to the active worker; graceful drain committed delta tokens; restarted worker resumed from checkpoint with **zero duplicate scans**.
* **Final Audit**: **5,000 / 5,000 files cataloged**, **150 threats quarantined**, **peak Heap: 106.4 MB**.

---

## 🚀 Quickstart

### Launch Full Docker Stack
```bash
./docker/start-oxguard.sh
```
This boots Redis 7, PostgreSQL 16, Bull-Board (port 3000), Prometheus (port 9090), Grafana (port 3001), Mock Office 365 Server (port 8081), and OxGuard 365 (port 8080).

Open your browser at 👉 **`http://localhost:8080`**.
