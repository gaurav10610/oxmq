package io.oxmq.oxguard.service;

import io.lettuce.core.api.sync.RedisCommands;
import io.oxmq.client.RedisConnectionManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import io.oxmq.oxguard.model.ScanOrigin;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.LongAdder;

/**
 * Approach 5: Distributed Tenant Cooldown Sentinel Service.
 * Manages Redis keys for tenant throttling cooldown and token bucket headroom yielding.
 */
@Service
public class TenantCooldownService {

    private static final Logger log = LoggerFactory.getLogger(TenantCooldownService.class);
    private static final String COOLDOWN_KEY_PREFIX = "oxguard:tenant:cooldown:";
    private static final String RATE_USAGE_KEY_PREFIX = "oxguard:tenant:usage:";

    private final RedisConnectionManager connectionManager;
    private final double headroomYieldThreshold;
    private final Map<String, AtomicInteger> tenantBurstLimits = new java.util.concurrent.ConcurrentHashMap<>();

    // Real-time API Quota Telemetry Counters partitioned by Tenant
    private final Map<String, LongAdder> forwardApiQuotaByTenant = new java.util.concurrent.ConcurrentHashMap<>();
    private final Map<String, LongAdder> backwardApiQuotaByTenant = new java.util.concurrent.ConcurrentHashMap<>();
    private final Map<String, LongAdder> backwardYieldByTenant = new java.util.concurrent.ConcurrentHashMap<>();
    private final Map<String, LongAdder> throttles429ByTenant = new java.util.concurrent.ConcurrentHashMap<>();
    private final Map<String, LongAdder> retriesRecoveredByTenant = new java.util.concurrent.ConcurrentHashMap<>();

    // Global Aggregate Counters
    private final LongAdder forwardApiQuotaUsed = new LongAdder();
    private final LongAdder backwardApiQuotaUsed = new LongAdder();
    private final LongAdder backwardYieldCount = new LongAdder();
    private final LongAdder throttles429HitCount = new LongAdder();
    private final LongAdder retriesRecoveredCount = new LongAdder();

    public TenantCooldownService(
            RedisConnectionManager connectionManager,
            @Value("${oxguard.rate-limit.headroom-yield-threshold:0.25}") double headroomYieldThreshold,
            @Value("${oxguard.rate-limit.max-requests-per-second:50}") int defaultBurstLimit
    ) {
        this.connectionManager = connectionManager;
        this.headroomYieldThreshold = headroomYieldThreshold;
        // Initialize default limits for all 5 official tenant profiles
        for (io.oxmq.oxguard.model.TenantProfile profile : io.oxmq.oxguard.model.TenantProfile.ALL_PROFILES) {
            this.tenantBurstLimits.put(profile.tenantId(), new AtomicInteger(profile.burstLimitRUsPerSec()));
        }
    }

    /**
     * Sets a tenant cooldown sentinel with TTL in Redis.
     */
    public void setCooldown(String tenantId, long cooldownMs) {
        try {
            RedisCommands<String, String> sync = connectionManager.sync();
            long ttlSeconds = Math.max(1, (cooldownMs + 999) / 1000L);
            long expiryTimestamp = System.currentTimeMillis() + cooldownMs;
            sync.setex(COOLDOWN_KEY_PREFIX + tenantId, ttlSeconds, String.valueOf(expiryTimestamp));
            log.warn("Approach 5 Sentinel SET: Tenant [{}] cooldown locked for {} ms in Redis", tenantId, cooldownMs);
        } catch (Exception e) {
            log.error("Failed setting tenant cooldown key in Redis: {}", e.getMessage());
        }
    }

    /**
     * Checks if the tenant is currently in a Graph-imposed cooldown.
     * Returns remaining milliseconds, or 0 if inactive.
     */
    public long getRemainingCooldownMs(String tenantId) {
        try {
            RedisCommands<String, String> sync = connectionManager.sync();
            String val = sync.get(COOLDOWN_KEY_PREFIX + tenantId);
            if (val != null) {
                long expiry = Long.parseLong(val);
                long remaining = expiry - System.currentTimeMillis();
                if (remaining > 0) {
                    return remaining;
                }
            }
        } catch (Exception e) {
            log.debug("Error checking tenant cooldown key in Redis: {}", e.getMessage());
        }
        return 0L;
    }

    /**
     * Records an API call for tenant token bucket tracking partitioned by ScanOrigin.
     */
    public void recordTenantApiCall(String tenantId, ScanOrigin origin) {
        if (origin == ScanOrigin.FORWARD) {
            forwardApiQuotaUsed.increment();
            forwardApiQuotaByTenant.computeIfAbsent(tenantId, k -> new LongAdder()).increment();
        } else {
            backwardApiQuotaUsed.increment();
            backwardApiQuotaByTenant.computeIfAbsent(tenantId, k -> new LongAdder()).increment();
        }

        try {
            RedisCommands<String, String> sync = connectionManager.sync();
            String key = RATE_USAGE_KEY_PREFIX + tenantId + ":" + (System.currentTimeMillis() / 1000L);
            long currentCount = sync.incr(key);
            if (currentCount == 1) {
                sync.expire(key, 5); // 5s TTL
            }
        } catch (Exception e) {
            log.debug("Error recording tenant API call: {}", e.getMessage());
        }
    }

    public void recordTenantApiCall(String tenantId) {
        recordTenantApiCall(tenantId, ScanOrigin.BACKWARD);
    }

    public void recordBackwardYield(String tenantId) {
        backwardYieldCount.increment();
        backwardYieldByTenant.computeIfAbsent(tenantId, k -> new LongAdder()).increment();
    }

    public void recordBackwardYield() {
        recordBackwardYield("tenant-mid-2500");
    }

    public void record429Hit(String tenantId) {
        throttles429HitCount.increment();
        throttles429ByTenant.computeIfAbsent(tenantId, k -> new LongAdder()).increment();
    }

    public void record429Hit() {
        record429Hit("tenant-mid-2500");
    }

    public void recordRetryRecovered(String tenantId) {
        retriesRecoveredCount.increment();
        retriesRecoveredByTenant.computeIfAbsent(tenantId, k -> new LongAdder()).increment();
    }

    public void recordRetryRecovered() {
        recordRetryRecovered("tenant-mid-2500");
    }

    public int getTenantBurstLimit(String tenantId) {
        return tenantBurstLimits.computeIfAbsent(
                tenantId,
                id -> new AtomicInteger(io.oxmq.oxguard.model.TenantProfile.getByIdOrDefault(id).burstLimitRUsPerSec())
        ).get();
    }

    /**
     * Checks if available token bucket capacity is below the yield threshold (< 25%).
     * If true, backward crawl jobs should proactively yield to prioritize forward scan jobs.
     */
    public boolean checkHeadroomYield(String tenantId) {
        try {
            RedisCommands<String, String> sync = connectionManager.sync();
            String key = RATE_USAGE_KEY_PREFIX + tenantId + ":" + (System.currentTimeMillis() / 1000L);
            String val = sync.get(key);
            if (val != null) {
                int used = Integer.parseInt(val);
                int limit = getTenantBurstLimit(tenantId);
                double remainingRatio = 1.0 - ((double) used / (double) limit);
                if (remainingRatio < headroomYieldThreshold) {
                    recordBackwardYield(tenantId);
                    log.info("Token bucket headroom low ({}% remaining for tenant [{}], used {}/{}). Yielding backward crawl!",
                            Math.round(remainingRatio * 100), tenantId, used, limit);
                    return true;
                }
            }
        } catch (Exception e) {
            log.debug("Error checking headroom yield: {}", e.getMessage());
        }
        return false;
    }

    public void setMaxRequestsPerSecond(String tenantId, int limit) {
        if (limit > 0) {
            tenantBurstLimits.computeIfAbsent(
                    tenantId,
                    id -> new AtomicInteger(io.oxmq.oxguard.model.TenantProfile.getByIdOrDefault(id).burstLimitRUsPerSec())
            ).set(limit);
            log.info("Tenant [{}] rate limit updated to {} req/s", tenantId, limit);
        }
    }

    public void setMaxRequestsPerSecond(int limit) {
        for (AtomicInteger val : tenantBurstLimits.values()) {
            val.set(limit);
        }
    }

    public int getMaxRequestsPerSecond() {
        return getTenantBurstLimit("tenant-mid-2500");
    }

    public Map<String, Object> getQuotaTelemetry(String tenantId) {
        Map<String, Object> map = new LinkedHashMap<>();
        int limit = getTenantBurstLimit(tenantId);
        int usedInSecond = 0;
        try {
            RedisCommands<String, String> sync = connectionManager.sync();
            String key = RATE_USAGE_KEY_PREFIX + tenantId + ":" + (System.currentTimeMillis() / 1000L);
            String val = sync.get(key);
            if (val != null) {
                usedInSecond = Integer.parseInt(val);
            }
        } catch (Exception ignored) {}

        int remainingTokens = Math.max(0, limit - usedInSecond);
        double remainingRatio = Math.max(0.0, 1.0 - ((double) usedInSecond / (double) limit));
        long cooldownRemaining = getRemainingCooldownMs(tenantId);

        long fwdUsed = forwardApiQuotaByTenant.computeIfAbsent(tenantId, k -> new LongAdder()).sum();
        long bwdUsed = backwardApiQuotaByTenant.computeIfAbsent(tenantId, k -> new LongAdder()).sum();
        long yields = backwardYieldByTenant.computeIfAbsent(tenantId, k -> new LongAdder()).sum();
        long throttles = throttles429ByTenant.computeIfAbsent(tenantId, k -> new LongAdder()).sum();
        long retries = retriesRecoveredByTenant.computeIfAbsent(tenantId, k -> new LongAdder()).sum();

        io.oxmq.oxguard.model.TenantProfile profile = io.oxmq.oxguard.model.TenantProfile.getByIdOrDefault(tenantId);

        map.put("tenantId", tenantId);
        map.put("organizationName", profile.organizationName());
        map.put("tier", profile.tier().name());
        map.put("tierDisplayName", profile.tier().getDisplayName());
        map.put("seatCount", profile.seatCount());
        map.put("maxRequestsPerSecond", limit);
        map.put("currentSecondUsage", usedInSecond);
        map.put("remainingTokens", remainingTokens);
        map.put("headroomYieldThreshold", headroomYieldThreshold);
        map.put("remainingHeadroomPercent", Math.round(remainingRatio * 1000.0) / 10.0);
        map.put("isYieldingActive", remainingRatio < headroomYieldThreshold);
        map.put("forwardApiQuotaUsed", fwdUsed);
        map.put("backwardApiQuotaUsed", bwdUsed);
        map.put("backwardYieldCount", yields);
        map.put("throttles429HitCount", throttles);
        map.put("retriesRecoveredCount", retries);
        map.put("is429CooldownActive", cooldownRemaining > 0);
        map.put("cooldownRemainingMs", cooldownRemaining);
        return map;
    }

    public Map<String, Object> getAllTenantsQuotaTelemetry() {
        Map<String, Object> all = new LinkedHashMap<>();
        for (io.oxmq.oxguard.model.TenantProfile p : io.oxmq.oxguard.model.TenantProfile.ALL_PROFILES) {
            all.put(p.tenantId(), getQuotaTelemetry(p.tenantId()));
        }
        all.put("globalForwardUsed", forwardApiQuotaUsed.sum());
        all.put("globalBackwardUsed", backwardApiQuotaUsed.sum());
        all.put("globalYieldCount", backwardYieldCount.sum());
        all.put("global429Hits", throttles429HitCount.sum());
        all.put("globalRetriesRecovered", retriesRecoveredCount.sum());
        return all;
    }
}

