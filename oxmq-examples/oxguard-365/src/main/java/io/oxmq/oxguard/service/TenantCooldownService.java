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
    private final AtomicInteger maxRequestsPerSecond;

    // Real-time API Quota Telemetry Counters
    private final LongAdder forwardApiQuotaUsed = new LongAdder();
    private final LongAdder backwardApiQuotaUsed = new LongAdder();
    private final LongAdder backwardYieldCount = new LongAdder();
    private final LongAdder throttles429HitCount = new LongAdder();
    private final LongAdder retriesRecoveredCount = new LongAdder();

    public TenantCooldownService(
            RedisConnectionManager connectionManager,
            @Value("${oxguard.rate-limit.headroom-yield-threshold:0.25}") double headroomYieldThreshold,
            @Value("${oxguard.rate-limit.max-requests-per-second:50}") int maxRequestsPerSecond
    ) {
        this.connectionManager = connectionManager;
        this.headroomYieldThreshold = headroomYieldThreshold;
        this.maxRequestsPerSecond = new AtomicInteger(maxRequestsPerSecond);
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
        } else {
            backwardApiQuotaUsed.increment();
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

    public void recordBackwardYield() {
        backwardYieldCount.increment();
    }

    public void record429Hit() {
        throttles429HitCount.increment();
    }

    public void recordRetryRecovered() {
        retriesRecoveredCount.increment();
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
                int limit = maxRequestsPerSecond.get();
                double remainingRatio = 1.0 - ((double) used / (double) limit);
                if (remainingRatio < headroomYieldThreshold) {
                    backwardYieldCount.increment();
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

    public void setMaxRequestsPerSecond(int limit) {
        if (limit > 0) {
            this.maxRequestsPerSecond.set(limit);
            log.info("Tenant rate limit updated to {} req/s", limit);
        }
    }

    public int getMaxRequestsPerSecond() {
        return maxRequestsPerSecond.get();
    }

    public Map<String, Object> getQuotaTelemetry(String tenantId) {
        Map<String, Object> map = new LinkedHashMap<>();
        int limit = maxRequestsPerSecond.get();
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

        map.put("maxRequestsPerSecond", limit);
        map.put("currentSecondUsage", usedInSecond);
        map.put("remainingTokens", remainingTokens);
        map.put("headroomYieldThreshold", headroomYieldThreshold);
        map.put("remainingHeadroomPercent", Math.round(remainingRatio * 1000.0) / 10.0);
        map.put("isYieldingActive", remainingRatio < headroomYieldThreshold);
        map.put("forwardApiQuotaUsed", forwardApiQuotaUsed.sum());
        map.put("backwardApiQuotaUsed", backwardApiQuotaUsed.sum());
        map.put("backwardYieldCount", backwardYieldCount.sum());
        map.put("throttles429HitCount", throttles429HitCount.sum());
        map.put("retriesRecoveredCount", retriesRecoveredCount.sum());
        map.put("is429CooldownActive", cooldownRemaining > 0);
        map.put("cooldownRemainingMs", cooldownRemaining);
        return map;
    }
}
