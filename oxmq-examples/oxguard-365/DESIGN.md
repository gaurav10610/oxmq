# OxGuard 365: Enterprise SaaS DSPM & Threat Analysis Engine
## High-Scale Architecture & Technical Specification (Java 17 LTS + OxMQ)

For specialized technical deep dives, see the comprehensive documentation guides:
* [⚡ Advanced Rate Limiting, Throttling & Adaptive Capacity](docs/RATE_LIMITING_AND_THROTTLING.md)
* [🛡️ Failure Handling & Disaster Recovery Guide](docs/FAILURE_HANDLING_AND_DISASTER_RECOVERY.md)
* [🏢 Multi-Tenant Architecture & Dynamic Licensing](docs/MULTI_TENANT_ARCHITECTURE.md)
* [🔬 Dual-Pillar Inspection DAG & Threat Engine](docs/DUAL_PILLAR_INSPECTION_ENGINE.md)
* [📐 Data Model, Schemas & LLD Specification](docs/DATA_MODEL_AND_LLD_SPECIFICATION.md)
* [📊 Observability, Metrics & Production Benchmarks](docs/OBSERVABILITY_AND_BENCHMARKS.md)

---

## 1. Executive Summary & Enterprise Scale Targets

**OxGuard 365** is an enterprise-grade SaaS Data Security Posture Management (DSPM) and file-level threat detection engine designed to inspect, catalog, and secure **SharePoint Sites** and **User OneDrives** at hyperscale.

### Scale & Performance SLA (Microsoft-Scale Baseline)
| Dimension | Enterprise Metric | Architectural Enforcement |
| :--- | :--- | :--- |
| **Tenant Scale** | 1,000+ Enterprise Tenants | Multi-tenant context isolation via OxMQ dynamic namespacing & tenant sharding. |
| **Monitored Scope** | 100,000+ Drives / Libraries | Parallel delta queries with persistent `@odata.deltaLink` checkpoints. |
| **Inventory Footprint** | 50,000,000+ Files | Partitioned PostgreSQL storage by `(tenant_id, drive_id)` + Redis cache. |
| **Sustained Throughput**| 10,000+ files / sec | Specialized OxMQ queue pipeline running on tuned Java 17 platform thread pools. |
| **Forward Latency** | $\le 30$ seconds (event to verdict) | Priority 1 scheduling + debounced webhook processing. |
| **Backward Impact** | Zero starvation of forward traffic | Dynamic token-bucket yielding ($<25\%$ capacity $\to$ backward deferral). |
| **Runtime Memory** | $< 512$ MB JVM Heap per worker pod | Zero-copy streaming downloads (`@microsoft.graph.downloadUrl` bypass). |

---

## 2. Microsoft Graph API Mechanics & Quota Hierarchy (100% Verified)

Microsoft Graph enforces a strict, hierarchical throttling model across three distinct tiers:
1. **Tier 1: Global App-Per-Tenant Quota:** ~10,000 requests per 10 minutes per App.
2. **Tier 2: Per-User / Per-Drive Quota:** ~1,200 requests per minute per User/Drive.
3. **Tier 3: Endpoint-Specific Limits:** Graph Gateway Bandwidth & Delta Token Volume.

### 2.1 Graph Endpoints Inventory
1. **SharePoint Discovery:**
   * `GET /v1.0/sites/getAllSites?$select=id,name,webUrl`: Discovers all site collections across the enterprise.
   * `GET /v1.0/sites/{site-id}/drives?$select=id,name,driveType,quota`: Enumerates all document libraries within each site.
2. **OneDrive Discovery:**
   * `GET /v1.0/users?$filter=accountEnabled eq true&$select=id,userPrincipalName`: Discovers all active enterprise users.
   * `GET /v1.0/users/{user-id}/drive`: Resolves each user's personal OneDrive document library.
3. **Change Tracking (Delta Queries):**
   * Initial enumeration: `GET /v1.0/drives/{drive-id}/root/delta` (follows `@odata.nextLink` until the final page returns `@odata.deltaLink`).
   * Incremental sync: `GET {saved-delta-link}` (returns only items modified, created, or deleted since the previous checkpoint).
4. **Permissions & Effective Exposure:**
   * `GET /v1.0/drives/{drive-id}/items/{item-id}/permissions`: Retrieves the complete Access Control List (ACL).
   * Evaluates `inheritedFrom` property: if present, the item inherits permissions from an ancestor folder; if absent, the item has **unique/direct permissions**.
5. **Content Streaming & Direct Azure Blob Bypass (Critical Optimization):**
   * Standard call: `GET /v1.0/drives/{drive-id}/items/{item-id}/content` (routes bytes through the Microsoft Graph gateway, heavily consuming Graph API quotas).
   * **Enterprise Bypass:** Query item metadata with `$select=@microsoft.graph.downloadUrl`. This returns a short-lived pre-authenticated SAS URL directly into Azure Blob storage. Downloading bytes directly from Azure Blob storage **bypasses Microsoft Graph API gateway limits completely**, preventing rate-limit exhaustion during heavy malware scans.

### 2.2 Webhook Subscriptions & Lifecycle Management
* **Target:** `POST /v1.0/subscriptions` with `resource: "/drives/{drive-id}/root"`, `changeType: "updated"`, and `expirationDateTime: now + 42300 minutes`.
* **Validation Handshake:** Microsoft Graph validates the webhook endpoint by sending a `POST` request with query param `validationToken=<string>`. The controller must immediately return HTTP 200 with the plain-text token in the response body within 10 seconds.
* **Notification Payload:** Notifications contain only metadata (`tenantId`, `clientState`, `resource`). They do not contain file deltas. The notification serves as an invalidation signal that immediately schedules a job on `drive-delta-sync-queue`.

---

## 3. Specialized Queues & Distributed Rate Limiting

OxGuard 365 pairs **domain-specialized queues** with a **unified tenant-level token bucket**:

| Queue Name | Primary Responsibility | Concurrency (Java 17) | Default Priority | Rate Limit Policy |
| :--- | :--- | :--- | :--- | :--- |
| `tenant-discovery-queue` | Discovers all Sites, Drives, and Users | 5 threads / pod | 5 (Normal) | Max 5 calls / sec per tenant |
| `drive-delta-sync-queue` | Executes `/delta` queries & ingests file deltas | 20 threads / pod | 1 (Forward) / 10 (Backward) | Bound to tenant group token bucket |
| `file-permission-queue` | Resolves direct vs. inherited ACLs & links | 30 threads / pod | Inherited from DAG | Bound to tenant group token bucket |
| `file-malware-queue` | Streams bytes from SAS URL & runs heuristics | 25 threads / pod | Inherited from DAG | Download concurrency cap (No Graph quota) |
| `file-aggregate-queue` | Merges exposure & threat data into PostgreSQL | 15 threads / pod | Inherited from DAG | Database write pool batching |

### Resilience Strategy: Approach 5 (`DelayedException` + Tenant Cooldown Sentinel)
* When Microsoft Graph returns HTTP 429 with `Retry-After: N`:
  1. Worker sets Redis key `tenant:{id}:cooldown` with TTL $N$.
  2. Worker throws `new DelayedException(N * 1000L, true)` (`skipAttempt = true`).
  3. All parallel workers inspect `tenant:{id}:cooldown` before making Graph API calls, deferring immediately without hitting the network.

---

## 4. Priority Arbitration & Debouncing

* **Forward Priority 1 vs Backward Priority 10:** Real-time webhook events process first.
* **Dynamic Headroom Yielding:** When tenant token bucket headroom $< 25\%$, backward crawl workers yield by throwing `DelayedException(5000L, true)`.
* **Auto-Save Debouncing:** 15s debounce window with `keepLastIfActive: true` collapses Office 365 auto-save bursts into 1 inspection job.

---

## 5. Dual-Pillar File Inspection Architecture

Every file item is processed via an **OxMQ `FlowProducer` DAG**:
* **Parent:** `file-metadata-node`
* **Children (Parallel):**
  * `file-permission-queue` (Pillar 1: Exposure Engine)
  * `file-malware-queue` (Pillar 2: Stream Scanner)
* **Aggregator:** `file-aggregate-queue`

### Pillar 1: Effective Exposure Engine
Classifies sharing permissions into 5 normalized tiers:
1. 🔴 `PUBLIC_ANONYMOUS`: Public sharing link.
2. 🟠 `EXTERNAL_GUEST`: Shared with external emails/guests.
3. 🟡 `ORGANIZATION_WIDE`: Tenant-wide link.
4. 🟢 `INTERNAL_RESTRICTED`: Specific internal users or security groups.
5. 🔒 `PRIVATE`: Owner only.

### Pillar 2: Zero-Heap Streaming Malware Scanner
Streams file bytes in 64 KB chunks through:
1. Real-time SHA-256 hash digest.
2. EICAR virus signature detector.
3. Shannon entropy calculator ($H > 7.5$ flagged as suspicious ransomware/packed).
4. MIME magic number mismatch detector (e.g. PE Windows Executable `MZ` header disguised as `.docx`).

---

## 6. Enterprise Mock Microsoft 365 Server & Local Docker Infrastructure

To enable 100% faithful testing on local developer machines without relying on paid Azure AD / Microsoft 365 enterprise tenant subscriptions, OxGuard 365 includes a dedicated **Enterprise Mock Microsoft 365 Server** and **Dockerized PostgreSQL persistence**:

### 6.1 Exact Microsoft Graph API v1.0 Contract
The mock server implements the exact HTTP wire protocol and JSON payload format of Microsoft Graph v1.0:
* **OAuth2 Token Exchange:** `POST /oauth2/v2.0/token` returns standard Azure AD Bearer tokens.
* **SharePoint Discovery:** `GET /v1.0/sites/getAllSites` and `GET /v1.0/sites/{siteId}/drives`.
* **OneDrive Discovery:** `GET /v1.0/users` and `GET /v1.0/users/{userId}/drive`.
* **Delta Sync:** `GET /v1.0/drives/{driveId}/root/delta` with pagination and delta links.
* **Permissions / ACL:** `GET /v1.0/drives/{driveId}/items/{itemId}/permissions`.
* **Binary File Streaming:** Serves physical files from disk under `./data/m365-storage` with realistic headers (`Content-Type`, `Content-Length`) and `@microsoft.graph.downloadUrl` SAS bypass.

> **Single Configuration Toggle:** In `application.yml`, the connector (`LiveGraphClient`) points to `graph-base-url: "http://localhost:8081/v1.0"`. To connect to live Microsoft cloud in production, only the hostname changes to `https://graph.microsoft.com/v1.0` while 100% of the connector and inspection code remains unchanged.

### 6.2 Dockerized PostgreSQL Database
* Run via `docker-compose.yml` (`docker compose up -d`):
  * `oxguard-postgres`: PostgreSQL 16 Alpine on port 5432 (`oxguard / oxguard`).
  * `oxguard-redis`: Redis 7 Alpine on port 6379.
* Managed via Spring Data JPA with entities:
  * `oxguard_scanned_files`: Storing file metadata, exposure levels, threat verdicts, SHA-256 hashes, and scan origins.
  * `oxguard_monitored_drives`: Tracking discovered SharePoint document libraries and user OneDrives.

