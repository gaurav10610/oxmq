package io.oxmq.oxguard.service;

import io.oxmq.oxguard.model.ScanOrigin;
import io.oxmq.oxguard.model.TenantProfile;
import io.oxmq.plugin.GroupRateLimiterPlugin;
import io.oxmq.plugin.PriorityQuotaGovernorPlugin;
import io.oxmq.plugin.RateLimitSentinelPlugin;
import java.time.Duration;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.atomic.LongAdder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

/**
 * Distributed Multi-Tenant Quota & Cooldown Orchestrator.
 * Powered natively by OxMQ's GroupRateLimiterPlugin, RateLimitSentinelPlugin, and PriorityQuotaGovernorPlugin.
 */
@Service
public class TenantCooldownService {

    private static final Logger log = LoggerFactory.getLogger(TenantCooldownService.class);

    private final GroupRateLimiterPlugin rateLimiter;
    private final RateLimitSentinelPlugin sentinel;
    private final PriorityQuotaGovernorPlugin governor;
    private final double headroomYieldThreshold;

    // Real-time API Quota Telemetry Counters partitioned by Tenant
    private final Map<String, LongAdder> forwardApiQuotaByTenant = new java.util.concurrent.ConcurrentHashMap<>();
    private final Map<String, LongAdder> backwardApiQuotaByTenant = new java.util.concurrent.ConcurrentHashMap<>();
    private final Map<String, LongAdder> retriesRecoveredByTenant = new java.util.concurrent.ConcurrentHashMap<>();

    // Global Aggregate Counters
    private final LongAdder forwardApiQuotaUsed = new LongAdder();
    private final LongAdder backwardApiQuotaUsed = new LongAdder();
    private final LongAdder retriesRecoveredCount = new LongAdder();

    public TenantCooldownService(
            GroupRateLimiterPlugin rateLimiter,
            RateLimitSentinelPlugin sentinel,
            PriorityQuotaGovernorPlugin governor,
            @Value("${oxguard.rate-limit.headroom-yield-threshold:0.25}") double headroomYieldThreshold
    ) {
        this.rateLimiter = rateLimiter;
        this.sentinel = sentinel;
        this.governor = governor;
        this.headroomYieldThreshold = headroomYieldThreshold;
    }

    /**
     * Sets a tenant cooldown sentinel with duration in Redis via OxMQ RateLimitSentinelPlugin.
     */
    public void setCooldown(String tenantId, long cooldownMs) {
        sentinel.triggerCooldown(tenantId, Duration.ofMillis(cooldownMs));
        log.warn("OxMQ RateLimitSentinel SET: Tenant [{}] cooldown locked for {} ms in Redis", tenantId, cooldownMs);
    }

    /**
     * Checks if the tenant is currently in a Graph-imposed cooldown via OxMQ RateLimitSentinelPlugin.
     * Returns remaining milliseconds, or 0 if inactive.
     */
    public long getRemainingCooldownMs(String tenantId) {
        return sentinel.getCooldownRemainingMs(tenantId);
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
    }

    public void recordTenantApiCall(String tenantId) {
        recordTenantApiCall(tenantId, ScanOrigin.BACKWARD);
    }

    public void recordBackwardYield(String tenantId) {
        // Governor handles counting yields natively
    }

    public void recordBackwardYield() {
        recordBackwardYield("tenant-mid-2500");
    }

    public void record429Hit(String tenantId) {
        // Sentinel handles counting throttles natively
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
        return rateLimiter.getLimit(tenantId);
    }

    public int getMaxRequestsPerSecond() {
        return getTenantBurstLimit("tenant-mid-2500");
    }

    public void setMaxRequestsPerSecond(String tenantId, int limit) {
        // Limit resolver in GroupRateLimiterPlugin resolves from TenantProfile or dynamic resolver
    }

    public void setMaxRequestsPerSecond(int limit) {
        // Dynamic overrides if configured
    }

    /**
     * Checks if available token bucket capacity is below the yield threshold (< 25%) via OxMQ PriorityQuotaGovernorPlugin.
     */
    public boolean checkHeadroomYield(String tenantId) {
        return governor.isYieldingActive(tenantId);
    }

    public Map<String, Object> getQuotaTelemetry(String tenantId) {
        Map<String, Object> map = new LinkedHashMap<>();
        int limit = rateLimiter.getLimit(tenantId);
        int usedInSecond = rateLimiter.getCurrentUsage(tenantId);
        int remainingTokens = rateLimiter.getRemainingTokens(tenantId);
        double remainingHeadroomPercent = rateLimiter.getRemainingHeadroomPercent(tenantId);
        long cooldownRemaining = sentinel.getCooldownRemainingMs(tenantId);
        boolean isYielding = governor.isYieldingActive(tenantId);
        long yields = governor.getYieldCount(tenantId);
        long throttles = sentinel.getThrottleCount(tenantId);

        long fwdUsed = forwardApiQuotaByTenant.computeIfAbsent(tenantId, k -> new LongAdder()).sum();
        long bwdUsed = backwardApiQuotaByTenant.computeIfAbsent(tenantId, k -> new LongAdder()).sum();
        long retries = retriesRecoveredByTenant.computeIfAbsent(tenantId, k -> new LongAdder()).sum();

        TenantProfile profile = TenantProfile.getByIdOrDefault(tenantId);

        map.put("tenantId", tenantId);
        map.put("organizationName", profile.organizationName());
        map.put("tier", profile.tier().name());
        map.put("tierDisplayName", profile.tier().getDisplayName());
        map.put("seatCount", profile.seatCount());
        map.put("maxRequestsPerSecond", limit);
        map.put("currentSecondUsage", usedInSecond);
        map.put("remainingTokens", remainingTokens);
        map.put("headroomYieldThreshold", headroomYieldThreshold);
        map.put("remainingHeadroomPercent", remainingHeadroomPercent);
        map.put("isYieldingActive", isYielding);
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
        long totalYields = 0;
        long totalThrottles = 0;
        for (TenantProfile p : TenantProfile.ALL_PROFILES) {
            Map<String, Object> telem = getQuotaTelemetry(p.tenantId());
            all.put(p.tenantId(), telem);
            totalYields += ((Number) telem.get("backwardYieldCount")).longValue();
            totalThrottles += ((Number) telem.get("throttles429HitCount")).longValue();
        }
        all.put("globalForwardUsed", forwardApiQuotaUsed.sum());
        all.put("globalBackwardUsed", backwardApiQuotaUsed.sum());
        all.put("globalYieldCount", totalYields);
        all.put("global429Hits", totalThrottles);
        all.put("globalRetriesRecovered", retriesRecoveredCount.sum());
        return all;
    }
}
