package io.oxmq.oxguard.service;

import io.oxmq.client.RedisConnectionManager;
import io.oxmq.oxguard.connector.GraphClient;
import io.oxmq.oxguard.model.TenantProfile;
import io.oxmq.oxguard.model.TenantTier;
import io.oxmq.oxguard.model.TierSource;
import jakarta.annotation.PostConstruct;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.LongAdder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;

/**
 * Dynamic Multi-Tenant Subscription and Adaptive Capacity Service.
 * Implements a 4-level fallback waterfall and Conservative AIMD Adaptive Capacity Tuning:
 * 1. Static Configuration / Admin API Override
 * 2. Microsoft Graph GET /v1.0/subscribedSkus (Seat Count & SKU)
 * 3. Heuristic User Count Fallback via GET /v1.0/users?$count=true (User.Read.All)
 * 4. Safe Baseline Fallback (Tier 1: SMB - 250 seats, 25 RUs/s)
 *
 * Automatically escalates (upgrades) or de-escalates (downgrades) tenant rate limits over time
 * while enforcing oscillation damping to prevent yo-yo throttling storms.
 */
@Service
public class TenantSubscriptionService {

    private static final Logger log = LoggerFactory.getLogger(TenantSubscriptionService.class);

    private final GraphClient graphClient;
    private final List<String> onboardedTenantIds;
    private final Map<String, TenantProfile> tenantRegistry = new ConcurrentHashMap<>();

    // Static Overrides (pinned by config or admin API)
    private final Map<String, TenantTier> staticOverrides = new ConcurrentHashMap<>();

    // Adaptive Capacity Controller State partitioned by Tenant
    private final Map<String, Long> lastDowngradeTimestamp = new ConcurrentHashMap<>();
    private final Map<String, AtomicInteger> consecutiveCleanWindows = new ConcurrentHashMap<>();
    private final Map<String, AtomicInteger> recent429Count = new ConcurrentHashMap<>();
    private final Map<String, LongAdder> workProcessedInWindow = new ConcurrentHashMap<>();
    private final Map<String, AtomicInteger> upgradeCounts = new ConcurrentHashMap<>();
    private final Map<String, AtomicInteger> downgradeCounts = new ConcurrentHashMap<>();

    @Value("${oxguard.adaptive-tuning.enabled:true}")
    private boolean adaptiveTuningEnabled = true;

    @Value("${oxguard.adaptive-tuning.damping-window-ms:60000}")
    private long dampingWindowMs = 60000; // Freeze upgrades for damping duration after downgrade

    @Value("${oxguard.adaptive-tuning.clean-windows-for-upgrade:2}")
    private int cleanWindowsForUpgrade = 2; // Required consecutive clean evaluation windows to step up

    @Autowired(required = false)
    private RedisConnectionManager redisConnectionManager;

    public TenantSubscriptionService(
            GraphClient graphClient,
            @Value("${oxguard.tenants.onboarded:tenant-smb-250,tenant-mid-2500,tenant-ent-10k,tenant-large-35k,tenant-global-100k}") List<String> onboardedTenantIds,
            @Value("#{${oxguard.tenants.overrides:{}}}") Map<String, String> configuredOverrides
    ) {
        this.graphClient = graphClient;
        this.onboardedTenantIds = onboardedTenantIds != null ? onboardedTenantIds : List.of();
        if (configuredOverrides != null) {
            configuredOverrides.forEach((tid, tierStr) -> {
                try {
                    staticOverrides.put(tid, TenantTier.valueOf(tierStr.trim().toUpperCase()));
                    log.info("Loaded static tier override from configuration for [{}]: {}", tid, tierStr);
                } catch (Exception e) {
                    log.warn("Invalid static override tier [{}] for tenant [{}]: {}", tierStr, tid, e.getMessage());
                }
            });
        }
    }

    @PostConstruct
    public void init() {
        log.info("Initializing Microsoft 365 Dynamic Tenant Subscription Registry with Adaptive Tuning (enabled: {})...", adaptiveTuningEnabled);
        for (String tenantId : onboardedTenantIds) {
            try {
                discoverAndRegister(tenantId);
            } catch (Exception e) {
                log.warn("Initial discovery deferred for tenant [{}] (will discover lazily): {}", tenantId, e.getMessage());
            }
        }
    }

    /**
     * Resolves tenant licensing and capacity limits using the 4-level waterfall hierarchy:
     * Level 1: Static Override
     * Level 2: Microsoft Graph /subscribedSkus
     * Level 3: Microsoft Graph /users count heuristic
     * Level 4: Safe baseline (Tier 1: SMB)
     */
    public TenantProfile discoverAndRegister(String tenantId) {
        if (tenantId == null || tenantId.isBlank()) {
            tenantId = "tenant-mid-2500";
        }

        // Level 1: Check Static / Admin Override
        if (staticOverrides.containsKey(tenantId)) {
            TenantTier overrideTier = staticOverrides.get(tenantId);
            TenantProfile profile = new TenantProfile(
                    tenantId,
                    "Pinned Tenant (" + tenantId + ")",
                    overrideTier,
                    overrideTier == TenantTier.SMB ? 250 : 10000,
                    overrideTier.getDailyRuBudget(),
                    overrideTier.getDefaultBurstLimitRUs(),
                    "STATIC_OVERRIDE",
                    TierSource.STATIC_OVERRIDE,
                    true,
                    upgradeCounts.computeIfAbsent(tenantId, k -> new AtomicInteger(0)).get(),
                    downgradeCounts.computeIfAbsent(tenantId, k -> new AtomicInteger(0)).get()
            );
            tenantRegistry.put(tenantId, profile);
            syncToRedis(profile);
            log.info("Assigned Level 1 Static Override for [{}] -> Tier: {} (Burst Limit: {} RUs/s, PINNED)",
                    tenantId, overrideTier.name(), overrideTier.getDefaultBurstLimitRUs());
            return profile;
        }

        // Level 2 & 3 & 4: Microsoft Graph Discovery Waterfall
        try {
            TenantProfile profile = graphClient.discoverTenantProfile(tenantId);
            if (profile != null) {
                // Preserve transition counts if previously tracked
                int ups = upgradeCounts.computeIfAbsent(tenantId, k -> new AtomicInteger(0)).get();
                int downs = downgradeCounts.computeIfAbsent(tenantId, k -> new AtomicInteger(0)).get();
                TenantProfile finalProfile = profile.withTier(profile.tier(), profile.tierSource(), false, ups, downs);
                tenantRegistry.put(tenantId, finalProfile);
                syncToRedis(finalProfile);
                log.info("Registered dynamic tenant subscription for [{}] -> Source: {}, Tier: {}, Seats: {}, Burst Limit: {} RUs/s",
                        tenantId, finalProfile.tierSource().name(), finalProfile.tier().name(), finalProfile.seatCount(), finalProfile.burstLimitRUsPerSec());
                return finalProfile;
            }
        } catch (Exception e) {
            log.warn("Graph API discovery exception for tenant [{}]: {}. Assigning safe baseline fallback.", tenantId, e.getMessage());
        }

        // Level 4: Safe Baseline Fallback (Tier 1: SMB)
        TenantTier safeTier = TenantTier.SMB;
        TenantProfile fallback = new TenantProfile(
                tenantId,
                "Tenant (" + tenantId + ")",
                safeTier,
                250,
                safeTier.getDailyRuBudget(),
                safeTier.getDefaultBurstLimitRUs(),
                "SAFE_BASELINE_FALLBACK",
                TierSource.SAFE_BASELINE_FALLBACK,
                false,
                0,
                0
        );
        tenantRegistry.put(tenantId, fallback);
        syncToRedis(fallback);
        log.warn("Applied Level 4 Safe Baseline Fallback for [{}] -> Tier: SMB (Burst Limit: 25 RUs/s)", tenantId);
        return fallback;
    }

    /**
     * Resolves the current tenant profile.
     */
    public TenantProfile getTenantProfile(String tenantId) {
        if (tenantId == null || tenantId.isBlank()) {
            tenantId = "tenant-mid-2500";
        }
        TenantProfile profile = tenantRegistry.get(tenantId);
        if (profile == null) {
            profile = discoverAndRegister(tenantId);
        }
        return profile;
    }

    /**
     * Resolves the dynamic token bucket burst limit (RUs/s) for a given tenant.
     * Evaluated on every job execution by OxMQ GroupRateLimiterPlugin.
     */
    public int getTenantBurstLimit(String tenantId) {
        return getTenantProfile(tenantId).burstLimitRUsPerSec();
    }

    /**
     * Returns all dynamically registered tenant profiles.
     */
    public List<TenantProfile> getAllTenants() {
        for (String id : onboardedTenantIds) {
            if (!tenantRegistry.containsKey(id)) {
                discoverAndRegister(id);
            }
        }
        return new ArrayList<>(tenantRegistry.values());
    }

    /**
     * Returns all onboarded tenant identifiers.
     */
    public List<String> getAllTenantIds() {
        return new ArrayList<>(onboardedTenantIds);
    }

    // =========================================================================
    // Static Override Administration
    // =========================================================================

    /**
     * Pins a static tier for an onboarded tenant account, bypassing dynamic discovery and adaptive tuning.
     */
    public TenantProfile setStaticOverride(String tenantId, TenantTier tier) {
        log.info("ADMIN OVERRIDE: Pinned static tier [{}] for tenant [{}]", tier.name(), tenantId);
        staticOverrides.put(tenantId, tier);
        return discoverAndRegister(tenantId);
    }

    /**
     * Removes a static override for an onboarded tenant account, re-enabling dynamic discovery and adaptive tuning.
     */
    public TenantProfile clearStaticOverride(String tenantId) {
        log.info("ADMIN OVERRIDE CLEARED: Re-enabling dynamic discovery and adaptive tuning for tenant [{}]", tenantId);
        staticOverrides.remove(tenantId);
        removeRedisOverride(tenantId);
        return discoverAndRegister(tenantId);
    }

    public boolean isOverridePinned(String tenantId) {
        return staticOverrides.containsKey(tenantId);
    }

    // =========================================================================
    // Adaptive Capacity Controller (Conservative AIMD)
    // =========================================================================

    /**
     * Records an HTTP 429 throttle event encountered on a tenant.
     * Immediately triggers an auto-downgrade and activates oscillation damping to freeze upgrades.
     */
    public synchronized void record429ThrottleEvent(String tenantId) {
        recent429Count.computeIfAbsent(tenantId, k -> new AtomicInteger(0)).incrementAndGet();

        if (staticOverrides.containsKey(tenantId)) {
            log.debug("Tenant [{}] is pinned with a static override; skipping adaptive downgrade.", tenantId);
            return;
        }

        TenantProfile current = getTenantProfile(tenantId);
        TenantTier prevTier = current.tier().previousTier();

        // Activate oscillation damping lock
        long dampingUntil = System.currentTimeMillis() + dampingWindowMs;
        lastDowngradeTimestamp.put(tenantId, dampingUntil);
        consecutiveCleanWindows.computeIfAbsent(tenantId, k -> new AtomicInteger(0)).set(0);

        if (prevTier != current.tier()) {
            int newDowns = downgradeCounts.computeIfAbsent(tenantId, k -> new AtomicInteger(0)).incrementAndGet();
            int ups = upgradeCounts.computeIfAbsent(tenantId, k -> new AtomicInteger(0)).get();

            TenantProfile downgraded = current.withTier(prevTier, TierSource.ADAPTIVE_TUNING, false, ups, newDowns);
            tenantRegistry.put(tenantId, downgraded);
            syncToRedis(downgraded);

            log.warn("🛑 ADAPTIVE AUTO-DOWNGRADE: Tenant [{}] throttled (HTTP 429). Downgraded from {} to {} (Burst limit: {} -> {} RUs/s). Upgrades frozen for {}s.",
                    tenantId, current.tier().name(), prevTier.name(), current.burstLimitRUsPerSec(), prevTier.getDefaultBurstLimitRUs(), dampingWindowMs / 1000);
        } else {
            log.warn("🛑 ADAPTIVE DAMPING: Tenant [{}] throttled (HTTP 429) at lowest tier (SMB: 25 RUs/s). Upgrades frozen for {}s.",
                    tenantId, dampingWindowMs / 1000);
        }
    }

    /**
     * Records active job processing activity for tenant work tracking.
     */
    public void recordWorkProcessed(String tenantId) {
        workProcessedInWindow.computeIfAbsent(tenantId, k -> new LongAdder()).increment();
    }

    /**
     * Evaluates a tenant's processing performance and adaptively steps up the tier if conditions are met.
     */
    public synchronized TenantProfile evaluateTenantTier(String tenantId) {
        if (!adaptiveTuningEnabled) {
            return getTenantProfile(tenantId);
        }

        if (staticOverrides.containsKey(tenantId)) {
            return getTenantProfile(tenantId);
        }

        TenantProfile current = getTenantProfile(tenantId);
        int throttles = recent429Count.computeIfAbsent(tenantId, k -> new AtomicInteger(0)).getAndSet(0);
        long work = workProcessedInWindow.computeIfAbsent(tenantId, k -> new LongAdder()).sumThenReset();

        // 1. If throttles occurred during this evaluation window, reset clean counter and do not upgrade
        if (throttles > 0) {
            consecutiveCleanWindows.computeIfAbsent(tenantId, k -> new AtomicInteger(0)).set(0);
            return current;
        }

        // 2. Check if oscillation damping is active
        long dampingUntil = lastDowngradeTimestamp.getOrDefault(tenantId, 0L);
        long now = System.currentTimeMillis();
        if (now < dampingUntil) {
            long remainingSec = (dampingUntil - now) / 1000;
            log.debug("Adaptive Tuning: Tenant [{}] in damping freeze ({}s remaining); skipping upgrade evaluation.", tenantId, remainingSec);
            return current;
        }

        // 3. Check for clean execution and backlog
        if (current.tier() == TenantTier.HYPERSCALE) {
            // Already at highest tier
            return current;
        }

        AtomicInteger cleanCounter = consecutiveCleanWindows.computeIfAbsent(tenantId, k -> new AtomicInteger(0));
        if (work > 0) {
            int clean = cleanCounter.incrementAndGet();
            log.debug("Adaptive Tuning: Tenant [{}] clean window {}/{} ({} jobs processed, 0 throttles)",
                    tenantId, clean, cleanWindowsForUpgrade, work);

            if (clean >= cleanWindowsForUpgrade) {
                // Conditions met: upgrade one tier above!
                TenantTier nextTier = current.tier().nextTier();
                int newUps = upgradeCounts.computeIfAbsent(tenantId, k -> new AtomicInteger(0)).incrementAndGet();
                int downs = downgradeCounts.computeIfAbsent(tenantId, k -> new AtomicInteger(0)).get();

                TenantProfile upgraded = current.withTier(nextTier, TierSource.ADAPTIVE_TUNING, false, newUps, downs);
                tenantRegistry.put(tenantId, upgraded);
                cleanCounter.set(0);
                syncToRedis(upgraded);

                log.info("🚀 ADAPTIVE AUTO-UPGRADE: Tenant [{}] sustained clean processing. Upgraded from {} to {} (Burst limit: {} -> {} RUs/s)!",
                        tenantId, current.tier().name(), nextTier.name(), current.burstLimitRUsPerSec(), nextTier.getDefaultBurstLimitRUs());
                return upgraded;
            }
        }

        return current;
    }

    /**
     * Periodic background evaluation task running every configured interval.
     */
    @Scheduled(fixedDelayString = "${oxguard.adaptive-tuning.evaluation-interval-ms:30000}")
    public void evaluateAdaptiveTuning() {
        if (!adaptiveTuningEnabled) {
            return;
        }
        for (String tenantId : getAllTenantIds()) {
            try {
                evaluateTenantTier(tenantId);
            } catch (Exception e) {
                log.warn("Error during adaptive tier evaluation for tenant [{}]: {}", tenantId, e.getMessage());
            }
        }
    }

    public int getConsecutiveCleanWindows(String tenantId) {
        return consecutiveCleanWindows.computeIfAbsent(tenantId, k -> new AtomicInteger(0)).get();
    }

    public int getUpgradeCount(String tenantId) {
        return upgradeCounts.computeIfAbsent(tenantId, k -> new AtomicInteger(0)).get();
    }

    public int getDowngradeCount(String tenantId) {
        return downgradeCounts.computeIfAbsent(tenantId, k -> new AtomicInteger(0)).get();
    }

    public void setAdaptiveTuningEnabled(boolean adaptiveTuningEnabled) {
        this.adaptiveTuningEnabled = adaptiveTuningEnabled;
    }

    public void setDampingWindowMs(long dampingWindowMs) {
        this.dampingWindowMs = dampingWindowMs;
    }

    public void setCleanWindowsForUpgrade(int cleanWindowsForUpgrade) {
        this.cleanWindowsForUpgrade = cleanWindowsForUpgrade;
    }

    public void resetDampingLock(String tenantId) {
        lastDowngradeTimestamp.remove(tenantId);
    }

    // =========================================================================
    // Redis Multi-Pod Synchronization (Graceful Degradation)
    // =========================================================================

    private void syncToRedis(TenantProfile profile) {
        if (redisConnectionManager == null) return;
        try {
            var conn = redisConnectionManager.getCommandConnection();
            String key = "oxguard:tenant:tier:" + profile.tenantId();
            conn.sync().hset(key, Map.of(
                    "tier", profile.tier().name(),
                    "source", profile.tierSource().name(),
                    "limit", String.valueOf(profile.burstLimitRUsPerSec()),
                    "pinned", String.valueOf(profile.isOverridePinned())
            ));
        } catch (Exception e) {
            log.debug("Redis state sync deferred: {}", e.getMessage());
        }
    }

    private void removeRedisOverride(String tenantId) {
        if (redisConnectionManager == null) return;
        try {
            var conn = redisConnectionManager.getCommandConnection();
            conn.sync().hdel("oxguard:tenant:tier:" + tenantId, "pinned");
        } catch (Exception ignored) {}
    }
}
