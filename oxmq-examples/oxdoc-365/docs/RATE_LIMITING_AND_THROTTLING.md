# ⚡ OxDoc 365: Advanced Rate Limiting, Throttling & Adaptive Capacity Architecture

---

## 1. Executive Summary & Problem Formulation

Scanning enterprise Microsoft 365 environments (SharePoint Sites and User OneDrives) for Document AI and content intelligence introduces a classic distributed systems dilemma:
1. **Massive Historical Backlog:** A typical mid-to-large enterprise holds millions of historical files across thousands of document libraries that require content extraction, metadata cataloging, and permission classification.
2. **Aggressive Cloud Gateways:** Microsoft Graph API enforces strict, multi-tiered rate limits across application, tenant, drive, and user boundaries. Violating these thresholds triggers HTTP `429 Too Many Requests` or HTTP `503 Service Unavailable`, locking out the application across the entire tenant.
3. **Real-Time Priority:** While background workers crawl historical files, active employees continuously upload, modify, and share documents. An urgent forward event (such as a document uploaded for instant processing) must be indexed within seconds—it cannot sit in a queue behind a 100,000-file historical backlog.
4. **The Thundering Herd Trap:** When naive queue systems encounter an HTTP 429 response, parallel worker threads continue hammering the API gateway. This exacerbates the lockout penalty, exhausts thread pools, causes memory bloat, and risks job loss in Dead Letter Queues (DLQ).

To solve this, **OxDoc 365** pairs **OxMQ native plugins** with an ultra-realistic **Multi-Tenant Token Bucket & AIMD Adaptive Controller**. This document provides an exhaustive, engineering-grade walkthrough of every algorithm, mathematical formula, Redis data structure, and exception handling mechanism governing rate limiting and throttling in OxDoc 365.

---

## 2. Microsoft Graph Multi-Tiered Throttling Architecture

Microsoft Graph does not employ a single flat rate limit. Instead, it enforces a hierarchical rate-limiting model:

```mermaid
flowchart TD
    subgraph M365_Gateway ["Microsoft Graph API Gateway (Hierarchical Limits)"]
        GlobalApp["1. Global Application Limit<br/>(~130,000 requests / 10s across all tenants)"]
        TenantApp["2. Per-App Per-Tenant Limit<br/>(Scaled by Tenant Paid License Seat Count: 25 to 250+ RUs/s)"]
        PerDrive["3. Per-Drive / Per-User Limit<br/>(~1,200 requests / minute per Document Library)"]
        CostCurrency["4. Resource Unit (RU) Valuation<br/>(Delta = 1 RU, Permissions = 1 RU, SAS Download = 0 RU)"]
    end

    GlobalApp --> TenantApp
    TenantApp --> PerDrive
    PerDrive --> CostCurrency
```

### 2.1 License & Seat Count Capacity Banding
Microsoft SharePoint Online explicitly scales the per-app per-tenant request budget over rolling windows based on the number of paid user licenses (seats) the organization owns:

| Tenant Tier | Seat Count | Daily RU Budget | Burst Capacity Limit | Throttling Sensitivity |
| :--- | :--- | :--- | :--- | :--- |
| **SMB / Free / Developer** | $250$ | $1,200,000\text{ RUs}$ | **$25\text{ RUs/sec}$** | High (easily saturated by single-worker bursts) |
| **Mid-Market Growth** | $2,500$ | $2,400,000\text{ RUs}$ | **$50\text{ RUs/sec}$** | Moderate |
| **Enterprise E3 / E5** | $10,000$ | $3,600,000\text{ RUs}$ | **$90\text{ RUs/sec}$** | Standard commercial enterprise |
| **Large Enterprise** | $35,000$ | $4,800,000\text{ RUs}$ | **$150\text{ RUs/sec}$** | Robust burst allowance |
| **Hyperscale Global** | $100,000$ | $6,000,000\text{ RUs}$ | **$250\text{ RUs/sec}$** | Extreme throughput capacity |

### 2.2 Resource Unit (RU) Cost Model
Not all operations carry equal weight at the Microsoft Graph gateway:
* **Delta Queries (`GET /v1.0/drives/{id}/root/delta`):** Cost **$1\text{ RU}$**.
* **Permission Enumeration (`GET /v1.0/drives/{id}/items/{id}/permissions`):** Costs **$1\text{ RU}$**.
* **Search Queries (`POST /v1.0/drives/{id}/root/search`):** Cost **$3\text{ RUs}$**.
* **Binary File Content Streaming (Bypass via SAS URL):** Costs **$0\text{ RUs}$** against Microsoft Graph. By retrieving the `@microsoft.graph.downloadUrl` pre-authenticated SAS URL, file bytes are streamed directly from Azure Blob Storage, completely bypassing Graph API gateway quotas.

---

## 3. The Multi-Plugin Interceptor Chain Architecture

In OxDoc 365, all rate limiting, backpressure, and circuit breaking logic is decoupled from business logic and encapsulated within **native OxMQ plugins** configured in [`OxDocPluginConfiguration.java`](file:///Users/gaurav/git/oxmq/oxmq-examples/oxdoc-365/src/main/java/io/oxmq/oxdoc/config/OxDocPluginConfiguration.java):

```mermaid
sequenceDiagram
    autonumber
    actor Worker as OxMQ Worker Thread
    participant Chain as OxMQ JobInterceptor Chain
    participant Sentinel as RateLimitSentinelPlugin
    participant Limiter as GroupRateLimiterPlugin
    participant Governor as PriorityQuotaGovernorPlugin
    participant Business as Worker Processor Method
    participant Redis as Redis 7 (Cluster State)

    Worker->>Chain: beforeProcess(job)
    Chain->>Sentinel: 1. Check active 429 cooldown?
    alt Cooldown Active in Redis
        Sentinel-->>Worker: InterceptorDecision.delay(remainingMs, true)
        Note over Worker: Job rescheduled in Redis without touching network!
    else Cooldown Inactive
        Sentinel->>Limiter: 2. Check token bucket quota
        alt Token Bucket Depleted
            Limiter-->>Worker: InterceptorDecision.delay(150ms, true)
        else Token Available
            Limiter->>Governor: 3. Check Headroom & Priority
            alt Priority 10 & Headroom < 25%
                Governor-->>Worker: InterceptorDecision.delay(200ms, true)
                Note over Worker: Background job yields to protect VIP forward traffic!
            else Allowed (Priority 1 or Headroom >= 25%)
                Governor-->>Business: Proceed to execute business logic
            end
        end
    end
```

---

## 4. Token Bucket Sliding Epoch Window Algorithm

The tenant rate limiter is implemented natively in `GroupRateLimiterPlugin` using an atomic sliding epoch window in Redis.

### 4.1 Redis Sliding Window Mechanics
Rather than maintaining memory-heavy sorted sets or complex Lua scripts for standard fixed windows, the limiter partitions token consumption into 1-second discrete epoch buckets:

$$\text{Redis Key} = \text{keyPrefix} + \text{":usage:"} + \text{tenantId} + \text{":"} + \left\lfloor \frac{t_{\text{current\_ms}}}{1000} \right\rfloor$$

```java
// Atomic token increment in Redis
long currentSecond = System.currentTimeMillis() / 1000L;
String redisKey = keyPrefix + ":usage:" + tenantId + ":" + currentSecond;
long count = sync.incr(redisKey);
if (count == 1) {
    sync.expire(redisKey, 5); // 5-second TTL auto-cleanup
}
```

### 4.2 Mathematical Refill Characteristics
* At millisecond $t = 1792318161999$, requests consume tokens from bucket `1792318161`.
* At millisecond $t = 1792318162000$, the system clock advances to the next second.
* The next request increments bucket `1792318162`, which begins at count $0$.
* **Refill Rate:** $100\%$ of the burst capacity quota $L_t$ is available at every 1-second epoch boundary.
* **Complexity:** $O(1)$ time complexity for both consumption check and key expiration.

---

## 5. AIMD Adaptive Capacity Controller (Additive Increase / Multiplicative Decrease)

When scanning multi-tenant cloud environments, hardcoded static rate limits inevitably fail:
* If the static limit is set too high, transient load spikes on Microsoft's end trigger cascading 429 lockouts.
* If the static limit is set too low, historical indexing takes days longer than necessary.

OxDoc 365 utilizes `AdaptiveRateLimiterPlugin` implementing **Additive Increase / Multiplicative Decrease (AIMD)** with **oscillation damping locks**.

```mermaid
stateDiagram-v2
    [*] --> InitialState: Init (Limit = 50 RUs/s)
    
    state "Clean Execution Window" as Clean {
        InitialState --> Clean: Window Success
        Clean --> Clean: Consecutive Clean Windows < Threshold
        Clean --> AdditiveIncrease: Clean Windows == Threshold (N=2)
    }

    state "Additive Increase" as AdditiveIncrease {
        AdditiveIncrease --> Clean: Lt+1 = min(Lmax, Lt + 25)
    }

    state "Multiplicative Decrease" as MultiplicativeDecrease {
        Clean --> MultiplicativeDecrease: HTTP 429 / 503 Encountered
        MultiplicativeDecrease --> DampingLock: Lt+1 = max(Lmin, floor(Lt * 0.5))
    }

    state "Damping Freeze Lock" as DampingLock {
        DampingLock --> DampingLock: Time Elapsed < Damping Window (60s)
        DampingLock --> Clean: Time Elapsed >= 60s (Lock Cleared)
    }
```

### 5.1 Mathematical Formulation of AIMD

#### Multiplicative Decrease (On HTTP 429 / 503 Fault):
When Microsoft Graph returns a throttling or gateway fault, the capacity limit is halved immediately:

$$L_{t+1} = \max\left(L_{\min}, \left\lfloor L_t \times \beta \right\rfloor\right)$$

Where:
* $L_t$ = Current rate limit for the tenant ($RUs/\text{sec}$).
* $\beta = 0.5$ (Backoff factor).
* $L_{\min} = 25\text{ RUs/sec}$ (Absolute minimum safe floor).

#### Additive Increase (On Sustained Clean Windows):
When a tenant sustains error-free processing across $N$ consecutive evaluation windows ($N = 2$, window duration $= 30\text{ seconds}$), capacity steps up incrementally:

$$L_{t+1} = \min\left(L_{\max}, L_t + \alpha\right)$$

Where:
* $\alpha = 25\text{ RUs/sec}$ (Additive increase step).
* $L_{\max} = 250\text{ RUs/sec}$ (Upper limit for Hyperscale E5 tier).

### 5.2 Oscillation Damping Lock
A major failure mode in naive AIMD implementations is **capacity fluttering**: a system cuts its limit on 429, immediately steps it back up on the next clean second, and promptly triggers another 429.

To eliminate oscillation, `AdaptiveRateLimiterPlugin` engages a **Damping Lock**:
* Upon recording a 429 throttle event, a Redis damping key is set:
  $$\text{Redis Key} = \text{"oxdoc:adaptive:damping:"} + \text{tenantId}$$
  with an expiration TTL equal to $\tau_{\text{damping}} = 60,000\text{ ms}$ ($60\text{ seconds}$).
* While `isDampingActive(tenantId) == true`, all additive upgrades are strictly frozen, even if worker throughput is $100\%$ clean.
* Upgrades can only resume after the damping period has fully elapsed, guaranteeing network stabilization.

### 5.3 Cluster State Synchronization via Redis
To ensure all distributed worker pods share identical adaptive limits, capacity is persisted in a cluster-wide Redis hash:

$$\text{Redis Hash} = \text{"oxdoc:adaptive:limits"}$$
$$\text{Field} = \text{tenantId}, \quad \text{Value} = L_{t+1}$$

Any pod updating a tenant's limit writes atomically to this hash, instantly synchronizing all worker nodes across the Kubernetes cluster.

---

## 6. Priority Quota Governor & Headroom Yielding

OxDoc 365 segregates all document processing traffic into two distinct priority classes:
1. **Forward Processing (Priority 1):** Real-time webhook notifications triggered by active user changes.
2. **Backward Crawl (Priority 10):** Historical discovery crawls ingesting large document backlogs.

### 6.1 Headroom Yield Formula
The `PriorityQuotaGovernorPlugin` continuously evaluates tenant quota headroom:

$$\text{Headroom}(t) = \frac{L_t - U_t}{L_t}$$

Where:
* $L_t$ = Current burst limit of the tenant ($RUs/\text{sec}$).
* $U_t$ = Tokens consumed within the current second.

### 6.2 The Yielding Condition
$$\text{If } \text{Priority} > 1 \quad \text{AND} \quad \text{Headroom}(t) < \theta_{\text{yield}} \implies \text{DEFER JOB}$$

Where:
* $\theta_{\text{yield}} = 0.25$ ($25\%$ reserve threshold).
* When remaining capacity drops below $25\%$, all Priority 10 background jobs yield immediately by returning:
  `InterceptorDecision.delay(200, true)`
* This strictly reserves the remaining $25\%$ of the tenant's API quota exclusively for Priority 1 forward webhook events, completely preventing historical batch crawls from starving real-time document events.

---

## 7. The Cooldown Sentinel Circuit Breaker (`RateLimitSentinelPlugin`)

When Microsoft Graph API returns an HTTP 429 response, it includes the mandatory IETF header:
`Retry-After: N` (e.g. `Retry-After: 5`).

### 7.1 Single-Trip Distributed Lockout
1. The first worker thread to encounter the 429 response catches `GraphThrottlingException` (which extends OxMQ's `RateLimitCooldownException`).
2. `RateLimitSentinelPlugin.onError()` captures the exception and atomically writes a cooldown key into Redis:
   $$\text{Redis Key} = \text{"oxmq:sentinel:tenant:cooldown:"} + \text{tenantId}$$
   $$\text{Value} = t_{\text{current}} + (N \times 1000), \quad \text{TTL} = N\text{ seconds}$$
3. The failing job is deferred for $N$ seconds via `moveToDelayed` with `skipAttempt = true`.

### 7.2 Zero-Network Preemptive Interception (Thundering Herd Elimination)
Before any other worker thread invokes its processor method for that tenant:
1. `RateLimitSentinelPlugin.beforeProcess(job)` queries `getCooldownRemainingMs(tenantId)`.
2. If `remainingMs > 0`, the plugin immediately returns:
   `InterceptorDecision.delay(remainingMs, true)`
3. **Outcome:** The job is non-destructively rescheduled in Redis. **Not a single HTTP request is sent over the wire**, completely shielding Microsoft Graph from thundering herds and allowing the tenant account to cool down cleanly.

---

## 8. Non-Destructive Job Deferrals: `moveToDelayed` vs. Traditional Anti-Patterns

A central innovation in OxMQ is its non-destructive job deferral model.

| Metric / Behavior | Traditional Queues (`Thread.sleep`) | Standard Retries (`throw Exception`) | OxMQ Native (`moveToDelayed`) |
| :--- | :--- | :--- | :--- |
| **Thread Utilization** | Blocked in memory for duration of sleep | Thread released, but error logged | Thread immediately returned to pool |
| **Worker Concurrency** | Starved; threads cannot process other tenants | Exhausted by rapid retry failures | $100\%$ available for unblocked tenants |
| **Retry Counter** | Unaffected (manual sleep) | Incremented (`attemptsMade++`); risks DLQ | **Not incremented** (`skipAttempt = true`) |
| **Lua Scripting** | N/A | Standard fail script | Official BullMQ `moveToDelayed-*.lua` |
| **Cluster Scalability** | Low (OOM under large thread pools) | Poor (causes DLQ poisoning) | **Linear scale (zero heap overhead)** |

In OxMQ, calling `moveToDelayed(delayMs, skipAttempt = true)` invokes upstream BullMQ Lua scripts directly. The job is placed in Redis `delayed` sorted set ordered by execution timestamp. The retry attempt count is preserved, guaranteeing that transient rate limiting never results in false-positive job failures or Dead Letter Queue poisoning.

---

## 9. Cooperative Multi-Tier Admission & Zero-Data-Loss Backpressure Gate

To guarantee total system stability across both external cloud API quotas and internal Redis memory budgets, OxDoc 365 chains 4 cooperative admission control mechanisms:

```mermaid
flowchart TD
    Job["Dispatched Job<br/>(drive-delta-sync-queue)"] --> L1{"1. RateLimitSentinel<br/>Active 429 Cooldown?"}
    
    L1 -- "Yes (HTTP 429)" --> D1["delay(remainingMs, skipAttempt=true)<br/>(Zero network calls)"]
    L1 -- "No" --> L2{"2. GroupRateLimiter<br/>Token Bucket Quota Available?"}
    
    L2 -- "Quota Depleted" --> D2["delay(150ms, skipAttempt=true)"]
    L2 -- "Quota OK" --> L3{"3. PriorityQuotaGovernor<br/>Priority 10 & Headroom < 25%?"}
    
    L3 -- "Yes (Low Headroom)" --> D3["delay(200ms, skipAttempt=true)<br/>(Protects Priority 1 Webhooks)"]
    L3 -- "Priority 1 OR Headroom >= 25%" --> L4{"4. BackpressureService<br/>RAM >= 170 MB OR Backlog >= 35k?"}
    
    L4 -- "Yes (PACING Active)" --> D4["delay(2500ms, skipAttempt=true)<br/>(Protects Strict 200 MB Ceiling)"]
    L4 -- "No (NORMAL)" --> Exec["Execute Worker Business Logic<br/>(Process Page & Emit DAG)"]
```

### 9.1 Memory Ceiling & Queue Backlog Protection
While Tiers 1-3 govern external Graph API quota consumption, Tier 4 ([`BackpressureService`](file:///Users/gaurav/git/oxmq/oxmq-examples/oxdoc-365/src/main/java/io/oxmq/oxdoc/service/BackpressureService.java)) governs internal cluster memory:
* **Ceiling Target:** Strict $200.0\text{ MB}$ Redis memory limit.
* **Dual Sensing:** Asynchronously polls Redis `used_memory` via Lettuce `INFO memory` and sums pending jobs across downstream extraction and categorization queues.
* **Zero Data Loss Pacing:** Paces delta continuation requests by $2,500\text{ ms}$ via `delay(2500ms, skipAttempt=true)`, allowing downstream child workers to drain extraction backlogs without consuming job retry attempts.
* **Real-Time Priority 1 Exemption:** Real-time user uploads carrying `ProcessingOrigin.FORWARD` are explicitly exempt from Tier 4 backpressure pacing, guaranteeing sub-second indexing latencies even when historical crawls are paced.
