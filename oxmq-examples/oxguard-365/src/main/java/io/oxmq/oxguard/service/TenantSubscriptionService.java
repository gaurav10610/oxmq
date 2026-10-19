package io.oxmq.oxguard.service;

import io.oxmq.client.RedisConnectionManager;
import io.oxmq.oxguard.connector.GraphClient;
import io.oxmq.oxguard.model.TenantProfile;
import io.oxmq.oxguard.model.TenantTier;
import io.oxmq.oxguard.model.TierSource;
import io.oxmq.plugin.AdaptiveRateLimiterPlugin;
import jakarta.annotation.PostConstruct;
import java.time.Duration;
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
 * Dynamic Multi-Tenant Subscription and Capacity Orchestration Service.
 * <p>
 * Leverages native OxMQ plugins ({@link AdaptiveRateLimiterPlugin}) for AIMD capacity tuning,
 * oscillation damping locks, and distributed multi-worker state synchronization.
 * <p>
 * Implements a 4-level fallback waterfall:
 * 1. Static Configuration / Admin API Override
 * 2. Microsoft Graph GET /v1.0/subscribedSkus (Seat Count & SKU)
 * 3. Heuristic User Count Fallback via GET /v1.0/users?$count=true (User.Read.All)
 * 4. Safe Baseline Fallback (Tier 1: SMB - 250 seats, 25 RUs/s)
 */
@Service
public class TenantSubscriptionService {

    private static final Logger log = LoggerFactory.getLogger(TenantSubscriptionService.class);

    private final GraphClient graphClient;
    private final List<String> onboardedTenantIds;
    private final Map<String, TenantProfile> tenantRegistry = new ConcurrentHashMap<>();
    private final Map<String, TenantTier> staticOverrides = new ConcurrentHashMap<>();

    private final Map<String, LongAdder> workProcessedInWindow = new ConcurrentHashMap<>();
    private final Map<String, AtomicInteger> recent429Count = new ConcurrentHashMap<>();

    @Value("${oxguard.adaptive-tuning.enabled:true}")
    private boolean adaptiveTuningEnabled = true;

    @Value("${oxguard.adaptive-tuning.damping-window-ms:60000}")
    private long dampingWindowMs = 60000;

    @Value("${oxguard.adaptive-tuning.clean-windows-for-upgrade:2}")
    private int cleanWindowsForUpgrade = 2;

    @Autowired(required = false)
    private RedisConnectionManager redisConnectionManager;

    @Autowired(required = false)
    private AdaptiveRateLimiterPlugin adaptiveRateLimiterPlugin;

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
        log.info("Initializing Microsoft 365 Dynamic Tenant Subscription Registry with Native OxMQ Adaptive Tuning (enabled: {})...", adaptiveTuningEnabled);
        for (String tenantId : onboardedTenantIds) {
            try {
                discoverAndRegister(tenantId);
            } catch (Exception e) {
                log.warn("Initial discovery deferred for tenant [{}] (will discover lazily): {}", tenantId, e.getMessage());
            }
        }
    }

    private AdaptiveRateLimiterPlugin getAdaptivePlugin() {
        if (adaptiveRateLimiterPlugin == null) {
            adaptiveRateLimiterPlugin = AdaptiveRateLimiterPlugin.builder()
                    .connectionManager(redisConnectionManager)
                    .keyPrefix("oxguard:adaptive")
                    .groupKeyResolver(job -> "tenant-mid-2500")
                    .initialLimit(50)
                    .minLimit(25)
                    .maxLimit(250)
                    .increaseStep(25)
                    .backoffFactor(0.5)
                    .dampingDuration(Duration.ofMillis(dampingWindowMs))
                    .cleanThreshold(cleanWindowsForUpgrade)
                    .build();
        }
        return adaptiveRateLimiterPlugin;
    }

    /**
     * Resolves tenant licensing and capacity limits using the 4-level waterfall hierarchy.
     */
    public TenantProfile discoverAndRegister(String tenantId) {
        if (tenantId == null || tenantId.isBlank()) {
            tenantId = "tenant-mid-2500";
        }

        // Level 1: Check Static / Admin Override
        if (staticOverrides.containsKey(tenantId)) {
            TenantTier overrideTier = staticOverrides.get(tenantId);
            getAdaptivePlugin().setStaticOverride(tenantId, overrideTier.getDefaultBurstLimitRUs());

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
                    getUpgradeCount(tenantId),
                    getDowngradeCount(tenantId)
            );
            tenantRegistry.put(tenantId, profile);
            log.info("Assigned Level 1 Static Override for [{}] -> Tier: {} (Burst Limit: {} RUs/s, PINNED)",
                    tenantId, overrideTier.name(), overrideTier.getDefaultBurstLimitRUs());
            return profile;
        }

        // Level 2 & 3 & 4: Microsoft Graph Discovery Waterfall
        try {
            TenantProfile profile = graphClient.discoverTenantProfile(tenantId);
            if (profile != null) {
                TenantProfile finalProfile = profile.withTier(profile.tier(), profile.tierSource(), false,
                        getUpgradeCount(tenantId), getDowngradeCount(tenantId));
                tenantRegistry.put(tenantId, finalProfile);
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
        log.warn("Applied Level 4 Safe Baseline Fallback for [{}] -> Tier: SMB (Burst Limit: 25 RUs/s)", tenantId);
        return fallback;
    }

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

    public int getTenantBurstLimit(String tenantId) {
        return getTenantProfile(tenantId).burstLimitRUsPerSec();
    }

    public List<TenantProfile> getAllTenants() {
        for (String id : onboardedTenantIds) {
            if (!tenantRegistry.containsKey(id)) {
                discoverAndRegister(id);
            }
        }
        return new ArrayList<>(tenantRegistry.values());
    }

    public List<String> getAllTenantIds() {
        return new ArrayList<>(onboardedTenantIds);
    }

    // =========================================================================
    // Static Override Administration
    // =========================================================================

    public TenantProfile setStaticOverride(String tenantId, TenantTier tier) {
        log.info("ADMIN OVERRIDE: Pinned static tier [{}] for tenant [{}]", tier.name(), tenantId);
        staticOverrides.put(tenantId, tier);
        getAdaptivePlugin().setStaticOverride(tenantId, tier.getDefaultBurstLimitRUs());
        return discoverAndRegister(tenantId);
    }

    public TenantProfile clearStaticOverride(String tenantId) {
        log.info("ADMIN OVERRIDE CLEARED: Re-enabling dynamic discovery and adaptive tuning for tenant [{}]", tenantId);
        staticOverrides.remove(tenantId);
        getAdaptivePlugin().clearStaticOverride(tenantId);
        return discoverAndRegister(tenantId);
    }

    public boolean isOverridePinned(String tenantId) {
        return staticOverrides.containsKey(tenantId);
    }

    // =========================================================================
    // Adaptive Capacity Controller (Powered by OxMQ AdaptiveRateLimiterPlugin)
    // =========================================================================

    public synchronized void record429ThrottleEvent(String tenantId) {
        recent429Count.computeIfAbsent(tenantId, k -> new AtomicInteger(0)).incrementAndGet();
        getAdaptivePlugin().recordThrottle(tenantId);

        if (staticOverrides.containsKey(tenantId)) {
            log.debug("Tenant [{}] is pinned with a static override; skipping adaptive downgrade.", tenantId);
            return;
        }

        TenantProfile current = getTenantProfile(tenantId);
        TenantTier prevTier = current.tier().previousTier();

        if (prevTier != current.tier()) {
            int newDowns = (int) getAdaptivePlugin().getDowngradeCount(tenantId);
            int ups = (int) getAdaptivePlugin().getUpgradeCount(tenantId);

            TenantProfile downgraded = current.withTier(prevTier, TierSource.ADAPTIVE_TUNING, false, ups, newDowns);
            tenantRegistry.put(tenantId, downgraded);

            log.warn("🛑 ADAPTIVE AUTO-DOWNGRADE: Tenant [{}] throttled (HTTP 429). Downgraded from {} to {} (Burst limit: {} -> {} RUs/s). Upgrades frozen for {}s.",
                    tenantId, current.tier().name(), prevTier.name(), current.burstLimitRUsPerSec(), prevTier.getDefaultBurstLimitRUs(), dampingWindowMs / 1000);
        } else {
            log.warn("🛑 ADAPTIVE DAMPING: Tenant [{}] throttled (HTTP 429) at lowest tier (SMB: 25 RUs/s). Upgrades frozen for {}s.",
                    tenantId, dampingWindowMs / 1000);
        }
    }

    public void recordWorkProcessed(String tenantId) {
        workProcessedInWindow.computeIfAbsent(tenantId, k -> new LongAdder()).increment();
    }

    public synchronized TenantProfile evaluateTenantTier(String tenantId) {
        if (!adaptiveTuningEnabled || staticOverrides.containsKey(tenantId)) {
            return getTenantProfile(tenantId);
        }

        TenantProfile current = getTenantProfile(tenantId);
        int throttles = recent429Count.computeIfAbsent(tenantId, k -> new AtomicInteger(0)).getAndSet(0);
        long work = workProcessedInWindow.computeIfAbsent(tenantId, k -> new LongAdder()).sumThenReset();

        if (throttles > 0) {
            return current;
        }

        if (getAdaptivePlugin().isDampingActive(tenantId)) {
            long remainingSec = getAdaptivePlugin().getDampingRemainingMs(tenantId) / 1000;
            log.debug("Adaptive Tuning: Tenant [{}] in damping freeze ({}s remaining); skipping upgrade evaluation.", tenantId, remainingSec);
            return current;
        }

        if (current.tier() == TenantTier.HYPERSCALE) {
            return current;
        }

        if (work > 0) {
            getAdaptivePlugin().recordCleanExecution(tenantId);
            int clean = getAdaptivePlugin().getConsecutiveCleanSuccesses(tenantId);

            if (clean == 0) {
                // Plugin triggered an upgrade!
                TenantTier nextTier = current.tier().nextTier();
                int newUps = (int) getAdaptivePlugin().getUpgradeCount(tenantId);
                int downs = (int) getAdaptivePlugin().getDowngradeCount(tenantId);

                TenantProfile upgraded = current.withTier(nextTier, TierSource.ADAPTIVE_TUNING, false, newUps, downs);
                tenantRegistry.put(tenantId, upgraded);

                log.info("🚀 ADAPTIVE AUTO-UPGRADE: Tenant [{}] sustained clean processing. Upgraded from {} to {} (Burst limit: {} -> {} RUs/s)!",
                        tenantId, current.tier().name(), nextTier.name(), current.burstLimitRUsPerSec(), nextTier.getDefaultBurstLimitRUs());
                return upgraded;
            }
        }

        return current;
    }

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
        return getAdaptivePlugin().getConsecutiveCleanSuccesses(tenantId);
    }

    public int getUpgradeCount(String tenantId) {
        return (int) getAdaptivePlugin().getUpgradeCount(tenantId);
    }

    public int getDowngradeCount(String tenantId) {
        return (int) getAdaptivePlugin().getDowngradeCount(tenantId);
    }

    public void setAdaptiveTuningEnabled(boolean adaptiveTuningEnabled) {
        this.adaptiveTuningEnabled = adaptiveTuningEnabled;
    }

    public void setDampingWindowMs(long dampingWindowMs) {
        this.dampingWindowMs = dampingWindowMs;
        this.adaptiveRateLimiterPlugin = null; // Recreate with updated damping
    }

    public void setCleanWindowsForUpgrade(int cleanWindowsForUpgrade) {
        this.cleanWindowsForUpgrade = cleanWindowsForUpgrade;
        this.adaptiveRateLimiterPlugin = null; // Recreate with updated threshold
    }

    public void resetDampingLock(String tenantId) {
        getAdaptivePlugin().resetDampingLock(tenantId);
    }
}
