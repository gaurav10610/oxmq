package io.oxmq.oxguard.service;

import io.lettuce.core.api.sync.RedisCommands;
import io.oxmq.client.RedisConnectionManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

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
    private final int maxRequestsPerSecond;

    public TenantCooldownService(
            RedisConnectionManager connectionManager,
            @Value("${oxguard.rate-limit.headroom-yield-threshold:0.25}") double headroomYieldThreshold,
            @Value("${oxguard.rate-limit.max-requests-per-second:50}") int maxRequestsPerSecond
    ) {
        this.connectionManager = connectionManager;
        this.headroomYieldThreshold = headroomYieldThreshold;
        this.maxRequestsPerSecond = maxRequestsPerSecond;
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
     * Records an API call for tenant token bucket tracking.
     */
    public void recordTenantApiCall(String tenantId) {
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
                double remainingRatio = 1.0 - ((double) used / (double) maxRequestsPerSecond);
                if (remainingRatio < headroomYieldThreshold) {
                    log.info("Token bucket headroom low ({}% remaining for tenant [{}]). Yielding backward crawl!",
                            Math.round(remainingRatio * 100), tenantId);
                    return true;
                }
            }
        } catch (Exception e) {
            log.debug("Error checking headroom yield: {}", e.getMessage());
        }
        return false;
    }
}
