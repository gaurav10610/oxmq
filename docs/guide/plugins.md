# Plugins & Interceptors SPI

OxMQ provides a non-intrusive, zero-overhead **Interceptor & Plugin Architecture** via `JobInterceptor`.

Plugins hook directly into the worker execution lifecycle:
- `beforeProcess(Job<T> job)`: Evaluates permissions, rate limits, quota headroom, or circuit breaker status before any worker thread invokes business code. Can return `InterceptorDecision.proceed()`, `InterceptorDecision.delay(durationMs, skipAttempt)`, or `InterceptorDecision.reject(reason)`.
- `afterProcess(Job<T> job, Duration executionDuration)`: Emits custom telemetry, writes audit logs, or updates external quota meters.
- `onError(Job<T> job, Throwable error, Duration duration)`: Intercepts exceptions (e.g. HTTP 429 Too Many Requests), sets distributed circuit breaker sentinels in Redis, and transparently defers jobs via `ErrorDecision.cooldown(duration, groupKey)` without wasting retry attempts.

> [!IMPORTANT] Upstream Lua Script Invariance
> All OxMQ plugins use standard BullMQ Lua primitives (`moveToDelayed-*.lua` with `skipAttempt=true`). No upstream Lua scripts are modified or diverged, preserving 100% byte-for-byte polyglot compatibility with Node.js and Python BullMQ workers.

---

## 🧩 Native Built-in Plugins

OxMQ provides three battle-tested enterprise plugins out of the box:

| Plugin | Class | Primary Purpose |
|---|---|---|
| **Group Rate Limiter** | `GroupRateLimiterPlugin` | Dynamic sliding window token bucket partitioned by custom group key (tenant, customer, API key). |
| **Circuit Breaker Sentinel** | `RateLimitSentinelPlugin` | Distributed upstream cooldown when external APIs signal HTTP 429 / 503 `Retry-After`. |
| **Priority Quota Governor** | `PriorityQuotaGovernorPlugin` | Headroom-based yielding: lower-priority background jobs automatically defer execution to reserve quota for real-time priority jobs. |

---

## 1. GroupRateLimiterPlugin (Multi-Tenant Isolation)

Enforces dynamic or static rate limits per tenant/group across all distributed cluster nodes.

```java
@Bean
public GroupRateLimiterPlugin groupRateLimiterPlugin(
        RedisConnectionManager connectionManager,
        MeterRegistry meterRegistry
) {
    return GroupRateLimiterPlugin.builder()
            .connectionManager(connectionManager)
            .keyPrefix("oxmq:limiter:tenant")
            .groupKeyResolver(job -> ((TenantScopedPayload) job.getData()).tenantId())
            .dynamicLimit(tenantId -> licenseTierService.getBurstLimit(tenantId)) // e.g. 50 req/sec
            .windowDuration(Duration.ofSeconds(1))
            .retryDelayMs(150)
            .meterRegistry(meterRegistry) // Emits 'oxmq.plugin.ratelimiter.yields'
            .build();
}
```

### Key Capabilities:
- **Dynamic Resolution**: Supports per-tenant limits that scale based on purchased licenses or dynamic subscription tiers.
- **Atomic Lettuce Pipeline**: Uses single round-trip atomic Redis INCR + TTL bucket algorithm.
- **Headroom Telemetry**: Query `getCurrentUsage(groupKey)`, `getRemainingTokens(groupKey)`, and `getRemainingHeadroomPercent(groupKey)` at any time.

---

## 2. RateLimitSentinelPlugin (External 429 Cooldown)

When third-party APIs (like Microsoft Graph, Salesforce, OpenAI, or Stripe) return HTTP 429 Too Many Requests, this plugin sets an atomic Redis cooldown lock. All workers across all distributed instances automatically defer jobs for that group without making redundant network calls.

```java
@Bean
public RateLimitSentinelPlugin rateLimitSentinelPlugin(
        RedisConnectionManager connectionManager,
        MeterRegistry meterRegistry
) {
    return RateLimitSentinelPlugin.builder()
            .connectionManager(connectionManager)
            .keyPrefix("oxmq:sentinel:tenant")
            .groupKeyResolver(job -> ((TenantScopedPayload) job.getData()).tenantId())
            .errorMatcher(t -> t instanceof RateLimitCooldownException)
            .meterRegistry(meterRegistry) // Emits 'oxmq.plugin.sentinel.delays' and 'oxmq.plugin.sentinel.cooldowns'
            .build();
}
```

Throwing `RateLimitCooldownException` from any worker triggers the distributed sentinel:

```java
if (httpResponse.statusCode() == 429) {
    long retryAfterSec = parseRetryAfter(httpResponse);
    throw new RateLimitCooldownException(Duration.ofSeconds(retryAfterSec), tenantId);
}
```

---

## 3. PriorityQuotaGovernorPlugin (Headroom-Based Yielding)

Prevents bulk background crawls (e.g. Priority 10 historical sync) from consuming quota needed by real-time interactive user actions (e.g. Priority 1 webhook events).

```java
@Bean
public PriorityQuotaGovernorPlugin priorityQuotaGovernorPlugin(
        GroupRateLimiterPlugin rateLimiter,
        MeterRegistry meterRegistry
) {
    return PriorityQuotaGovernorPlugin.builder()
            .rateLimiter(rateLimiter)
            .groupKeyResolver(job -> ((TenantScopedPayload) job.getData()).tenantId())
            .headroomYieldThreshold(0.25) // If < 25% token headroom remains
            .priorityThreshold(1)         // Priority <= 1 jobs are strictly protected!
            .yieldDelayMs(200)            // Low-priority jobs yield and defer 200ms
            .meterRegistry(meterRegistry) // Emits 'oxmq.plugin.governor.yields'
            .build();
}
```

---

## 📊 Prometheus & Grafana Telemetry

All plugins natively export real-time metrics to Micrometer:

- `oxmq.plugin.ratelimiter.yields` (Counter, tags: `group`, `queue`)
- `oxmq.plugin.sentinel.delays` (Counter, tags: `group`, `queue`)
- `oxmq.plugin.sentinel.cooldowns` (Counter, tags: `group`)
- `oxmq.plugin.governor.yields` (Counter, tags: `group`, `queue`)

### Bundled Dashboard JSON
A complete Grafana dashboard template is bundled directly in the starter:
`oxmq-spring-boot-starter/src/main/resources/dashboards/oxmq-grafana-dashboard.json`.
Import this file into any Grafana instance to monitor real-time queue throughput, backlog depth, duration percentiles, and multi-tenant quota yields.
