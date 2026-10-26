# 📐 OxDoc 365: Data Model, Schemas & Low-Level Design (LLD) Specification

---

## 1. Executive Summary

This document provides the definitive technical specification of OxDoc 365's data persistence layer, Redis key topology, Java domain transfer objects (DTOs), and REST/Webhook API contracts.

---

## 2. PostgreSQL 16 Relational Schema

OxDoc 365 uses Spring Data JPA with PostgreSQL 16. The database stores discovered monitored drives and the cataloged document intelligence inventory.

```mermaid
erDiagram
    oxdoc_monitored_drives ||--o{ oxdoc_indexed_documents : contains
    
    oxdoc_monitored_drives {
        varchar drive_id PK "Primary Key (e.g. drive-ent-sp-audits)"
        varchar tenant_id "Tenant Identifier"
        varchar drive_type "SHAREPOINT_DOCUMENT_LIBRARY | USER_ONEDRIVE"
        varchar name "Drive or Document Library Name"
        varchar owner_principal "Owner UPN or Site Administrator"
        varchar delta_token "Saved @odata.deltaLink Checkpoint"
        varchar webhook_subscription_id "Azure Subscription GUID"
    }

    oxdoc_indexed_documents {
        bigserial id PK "Auto-incrementing surrogate ID"
        varchar document_id UK "Unique Graph item ID"
        varchar drive_id "FK reference to monitored drive"
        varchar tenant_id "Tenant context identifier"
        varchar file_name "Document file name with extension"
        varchar file_path "Full folder hierarchy path"
        bigint file_size "File size in bytes"
        varchar mime_type "MIME content type"
        varchar access_scope "PUBLIC_ANONYMOUS | ORGANIZATION_WIDE | INTERNAL_RESTRICTED | CONFIDENTIAL_RESTRICTED | PRIVATE_INDIVIDUAL"
        boolean has_unique_permissions "True if ACL breaks folder inheritance"
        varchar indexing_verdict "INDEXED_SUCCESS | UNSUPPORTED_FORMAT | EXTRACTION_FAILED | CORRUPTED_ARCHIVE | SKIPPED_BINARY"
        varchar extracted_text_preview "Extracted text summary or extraction notes"
        varchar content_hash "SHA-256 cryptographic content digest"
        varchar processing_origin "FORWARD | BACKWARD"
        bigint indexed_at_timestamp "Epoch millisecond indexing timestamp"
    }
```

### 2.1 Complete PostgreSQL DDL
```sql
-- 1. Discovered Document Libraries & User OneDrives
CREATE TABLE IF NOT EXISTS oxdoc_monitored_drives (
    drive_id VARCHAR(255) PRIMARY KEY,
    tenant_id VARCHAR(255) NOT NULL,
    drive_type VARCHAR(64) NOT NULL,
    name VARCHAR(255) NOT NULL,
    owner_principal VARCHAR(255),
    delta_token VARCHAR(2048),
    webhook_subscription_id VARCHAR(255)
);

CREATE INDEX IF NOT EXISTS idx_drive_tenant ON oxdoc_monitored_drives(tenant_id);

-- 2. Content-Indexed Documents Inventory
CREATE TABLE IF NOT EXISTS oxdoc_indexed_documents (
    id BIGSERIAL PRIMARY KEY,
    document_id VARCHAR(255) NOT NULL UNIQUE,
    drive_id VARCHAR(255) NOT NULL,
    tenant_id VARCHAR(255) NOT NULL,
    file_name VARCHAR(255) NOT NULL,
    file_path VARCHAR(1024) NOT NULL,
    file_size BIGINT NOT NULL,
    mime_type VARCHAR(255),
    access_scope VARCHAR(64) NOT NULL,
    has_unique_permissions BOOLEAN NOT NULL DEFAULT FALSE,
    indexing_verdict VARCHAR(64) NOT NULL,
    extracted_text_preview VARCHAR(1024),
    content_hash VARCHAR(64),
    processing_origin VARCHAR(32) NOT NULL,
    indexed_at_timestamp BIGINT NOT NULL
);

-- Strategic Query Indexes
CREATE INDEX IF NOT EXISTS idx_document_id ON oxdoc_indexed_documents(document_id);
CREATE INDEX IF NOT EXISTS idx_access_scope ON oxdoc_indexed_documents(access_scope);
CREATE INDEX IF NOT EXISTS idx_indexing_verdict ON oxdoc_indexed_documents(indexing_verdict);
CREATE INDEX IF NOT EXISTS idx_processing_origin ON oxdoc_indexed_documents(processing_origin);
CREATE INDEX IF NOT EXISTS idx_indexed_at ON oxdoc_indexed_documents(indexed_at_timestamp);
CREATE INDEX IF NOT EXISTS idx_doc_tenant ON oxdoc_indexed_documents(tenant_id);
```

---

## 3. Distributed Redis Key Topology

OxDoc 365 and OxMQ utilize Redis 7 for queue management, distributed token buckets, circuit breakers, and delta sync checkpoints:

| Redis Key Pattern | Data Type | TTL | Owning Component | Description |
| :--- | :--- | :--- | :--- | :--- |
| `bull:<queueName>:id` | `String (integer)` | None | BullMQ Core | Atomic counter generating unique sequential job IDs. |
| `bull:<queueName>:wait` | `List` | None | BullMQ Core | FIFO queue of pending jobs awaiting worker dispatch. |
| `bull:<queueName>:active` | `List` | None | BullMQ Core | In-flight jobs currently executing on worker threads. |
| `bull:<queueName>:delayed` | `ZSet` | None | BullMQ Core | Jobs scheduled to run in the future (sorted by execution epoch ms). |
| `bull:<queueName>:completed` | `ZSet` | None | BullMQ Core | History of successfully completed jobs. |
| `bull:<queueName>:failed` | `ZSet` | None | BullMQ Core | Exhausted or unhandled failed jobs. |
| `oxmq:limiter:tenant:usage:<tenantId>:<epochSec>` | `String (integer)` | $5\text{ seconds}$ | `GroupRateLimiterPlugin` | Sliding epoch token bucket tracking RUs consumed within current second. |
| `oxmq:sentinel:tenant:cooldown:<tenantId>` | `String (timestamp)` | $N\text{ seconds}$ | `RateLimitSentinelPlugin` | Distributed circuit breaker locked upon encountering HTTP 429 `Retry-After: N`. |
| `oxdoc:adaptive:limits` | `Hash` | None | `AdaptiveRateLimiterPlugin` | Cluster-wide synchronized adaptive capacity limits per tenant ($L_t$). |
| `oxdoc:adaptive:damping:<tenantId>` | `String` | $60\text{ seconds}$ | `AdaptiveRateLimiterPlugin` | Damping lock freezing additive upgrades following an HTTP 429/503 fault. |
| `oxdoc:checkpoint:drives` | `Hash` | None | `CheckpointStatePlugin` | High-speed cache of latest `@odata.deltaLink` tokens mapped by `driveId`. |

---

## 4. Domain Payload Specifications (Java Records & JSON)

### 4.1 `DriveDeltaSyncPayload`
Transferred across `drive-delta-sync-queue`:
```java
public record DriveDeltaSyncPayload(
        String tenantId,
        String driveId,
        ProcessingOrigin processingOrigin // FORWARD (Priority 1) or BACKWARD (Priority 10)
) implements TenantScopedPayload {}
```
*JSON Representation:*
```json
{
  "tenantId": "tenant-ent-10k",
  "driveId": "drive-ent-sp-audits",
  "processingOrigin": "BACKWARD"
}
```

### 4.2 `DocumentProcessingPayload`
Transferred to child DAG workers (`content-categorization-queue` and `document-extraction-queue`):
```java
public record DocumentProcessingPayload(
        String tenantId,
        String driveId,
        String itemId,
        String itemName,
        String itemPath,
        long fileSize,
        String mimeType,
        String downloadUrl,
        ProcessingOrigin processingOrigin
) implements TenantScopedPayload {}
```

### 4.3 `DocumentCatalogPayload`
Transferred to parent DAG worker (`catalog-index-queue`):
```java
public record DocumentCatalogPayload(
        String tenantId,
        String driveId,
        String itemId,
        String itemName,
        String itemPath,
        long fileSize,
        String mimeType,
        String downloadUrl,
        ProcessingOrigin processingOrigin
) implements TenantScopedPayload {}
```

---

## 5. REST API & Webhook Endpoint Contracts

### 5.1 Multi-Tenant Management (`/api/v1/dashboard`)

#### `GET /api/v1/dashboard/tenants`
Lists all onboarded tenants, active license tiers, and live token usage.
*Response (HTTP 200 OK):*
```json
[
  {
    "tenantId": "tenant-global-100k",
    "organizationName": "MegaCorp Global",
    "tier": "HYPERSCALE",
    "tierDisplayName": "Hyperscale Global (100,000 seats)",
    "seatCount": 100000,
    "dailyRuBudget": 6000000,
    "burstLimitRUsPerSec": 250,
    "tierSource": "GRAPH_SKU_DISCOVERY",
    "isOverridePinned": false,
    "upgradeCount": 0,
    "downgradeCount": 0,
    "connected": true
  }
]
```

#### `GET /api/v1/dashboard/stats`
Returns system-wide document intelligence statistics:
*Response (HTTP 200 OK):*
```json
{
  "totalDocuments": 15000,
  "searchableDocuments": 14250,
  "externalShared": 350,
  "publicAnonymous": 12,
  "totalDrives": 25,
  "activeTenants": 5
}
```

#### `GET /api/v1/dashboard/backpressure`
Returns instantaneous Redis memory utilization, downstream queue backlog depths, and backpressure pacing status:
*Response (HTTP 200 OK):*
```json
{
  "enabled": true,
  "redisUsedMemoryMB": 44.9,
  "redisMaxMemoryCeilingMB": 200.0,
  "memoryUtilizationPercent": 22.45,
  "downstreamBacklogJobs": 1240,
  "isBackpressureActive": false,
  "totalYields": 1,
  "lastThrottleReason": null,
  "lastEvaluatedAt": "2026-10-27T01:10:00Z"
}
```

#### `GET /api/v1/dashboard/stream`
Server-Sent Events (SSE) telemetry stream emitting real-time JSON payloads every 1,500 ms for live UI updates:
*Event Stream Output:*
```
event: metrics
data: {"totalDocuments":30817,"searchableDocuments":29850,"externalShared":840,"publicAnonymous":127,"redisUsedMemoryMB":44.9,"redisMaxMemoryCeilingMB":200.0,"isBackpressureActive":false,"downstreamBacklogJobs":1240}
```

#### `POST /api/v1/dashboard/tenants/{tenantId}/override?tier={TIER}`
Pins a static administrative override on a tenant, bypassing adaptive auto-tuning.
*Request Example:* `POST /api/v1/dashboard/tenants/tenant-smb-250/override?tier=HYPERSCALE`
*Response (HTTP 200 OK):* Returns updated `TenantProfile` with `isOverridePinned = true`.

#### `DELETE /api/v1/dashboard/tenants/{tenantId}/override`
Clears administrative override, re-enabling dynamic Graph discovery and adaptive tuning.

#### `POST /api/v1/dashboard/trigger/crawl-all`
Dispatches historical baseline discovery across all 5 onboarded tenants concurrently.

---

### 5.2 Microsoft Graph Webhook Lifecycle (`/api/v1/webhooks/graph`)

#### `GET /api/v1/webhooks/graph?validationToken={token}`
*Handshake Protocol:* Microsoft Graph issues this verification call when creating a subscription. The endpoint immediately replies with HTTP 200 and the plain-text `validationToken` in the response body.

#### `POST /api/v1/webhooks/graph`
*Ingestion Protocol:* Receives resource change invalidation notifications from Microsoft Graph.
*Request Payload:*
```json
{
  "value": [
    {
      "subscriptionId": "7f9e8a7b-6c5d-4e3f-2a1b-0c9d8e7f6a5b",
      "clientState": "oxdoc-secret-state-token",
      "changeType": "updated",
      "resource": "drives/drive-ent-sp-audits/root",
      "tenantId": "tenant-ent-10k"
    }
  ]
}
```
*Processing Action:* Extracts `driveId` and dispatches a Priority 1 forward indexing job to `drive-delta-sync-queue` with 15-second debounce deduplication.
