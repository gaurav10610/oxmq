package io.oxmq.oxdoc.service;

import io.oxmq.oxdoc.connector.GraphClient;
import io.oxmq.oxdoc.model.TenantProfile;
import io.oxmq.oxdoc.model.TenantTier;
import io.oxmq.oxdoc.model.TierSource;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
public class AdaptiveTenantTierControllerTest {

    @Mock
    private GraphClient graphClient;

    private TenantSubscriptionService subscriptionService;

    @BeforeEach
    void setUp() {
        subscriptionService = new TenantSubscriptionService(
                graphClient,
                List.of("tenant-smb-250", "tenant-mid-2500"),
                Map.of()
        );
        subscriptionService.setAdaptiveTuningEnabled(true);
        subscriptionService.setCleanWindowsForUpgrade(2);
        subscriptionService.setDampingWindowMs(5000);
    }

    @Test
    @DisplayName("Level 1: Static override pinning enforces fixed tier and protects against 429 downgrade")
    void testStaticOverridePinningAndDowngradeProtection() {
        String tenantId = "tenant-pinned-test";

        TenantProfile pinned = subscriptionService.setStaticOverride(tenantId, TenantTier.LARGE_ENTERPRISE);
        assertNotNull(pinned);
        assertEquals(TenantTier.LARGE_ENTERPRISE, pinned.tier());
        assertEquals(TierSource.STATIC_OVERRIDE, pinned.tierSource());
        assertTrue(pinned.isOverridePinned());
        assertEquals(150, pinned.burstLimitRUsPerSec());
        assertEquals(150, subscriptionService.getTenantBurstLimit(tenantId));

        // 429 throttle event should NOT downgrade a pinned static override
        subscriptionService.record429ThrottleEvent(tenantId);
        TenantProfile after429 = subscriptionService.getTenantProfile(tenantId);
        assertEquals(TenantTier.LARGE_ENTERPRISE, after429.tier());
        assertTrue(after429.isOverridePinned());

        // Clear static override
        when(graphClient.discoverTenantProfile(tenantId)).thenReturn(new TenantProfile(
                tenantId,
                "Dynamic Org",
                TenantTier.MID_MARKET,
                2500,
                TenantTier.MID_MARKET.getDailyRuBudget(),
                TenantTier.MID_MARKET.getDefaultBurstLimitRUs(),
                "M365_BUSINESS_PREMIUM",
                TierSource.GRAPH_SKU_DISCOVERY,
                false,
                0,
                0
        ));

        TenantProfile restored = subscriptionService.clearStaticOverride(tenantId);
        assertFalse(restored.isOverridePinned());
        assertEquals(TenantTier.MID_MARKET, restored.tier());
        assertEquals(TierSource.GRAPH_SKU_DISCOVERY, restored.tierSource());
        assertEquals(50, subscriptionService.getTenantBurstLimit(tenantId));
    }

    @Test
    @DisplayName("Level 4: Safe baseline fallback (Tier 1 SMB) when Graph API calls fail")
    void testLevel4SafeBaselineFallbackWhenGraphFails() {
        String tenantId = "unreachable-tenant-403";
        when(graphClient.discoverTenantProfile(tenantId))
                .thenThrow(new RuntimeException("403 Forbidden: Missing Organization.Read.All scope"));

        TenantProfile fallback = subscriptionService.discoverAndRegister(tenantId);
        assertNotNull(fallback);
        assertEquals(TenantTier.SMB, fallback.tier());
        assertEquals(TierSource.SAFE_BASELINE_FALLBACK, fallback.tierSource());
        assertEquals(250, fallback.seatCount());
        assertEquals(25, fallback.burstLimitRUsPerSec());
        assertEquals(25, subscriptionService.getTenantBurstLimit(tenantId));
        assertFalse(fallback.isOverridePinned());
    }

    @Test
    @DisplayName("Adaptive Tuning: Auto-upgrade steps up after consecutive clean execution windows")
    void testAdaptiveAutoUpgradeAfterCleanWindows() {
        String tenantId = "tenant-adaptive-up";
        when(graphClient.discoverTenantProfile(tenantId)).thenReturn(new TenantProfile(
                tenantId,
                "Adaptive Corp",
                TenantTier.SMB,
                250,
                TenantTier.SMB.getDailyRuBudget(),
                TenantTier.SMB.getDefaultBurstLimitRUs(),
                "SAFE_BASELINE_FALLBACK",
                TierSource.SAFE_BASELINE_FALLBACK,
                false,
                0,
                0
        ));

        subscriptionService.discoverAndRegister(tenantId);
        assertEquals(TenantTier.SMB, subscriptionService.getTenantProfile(tenantId).tier());
        assertEquals(25, subscriptionService.getTenantBurstLimit(tenantId));

        // Window 1: Process work cleanly
        subscriptionService.recordWorkProcessed(tenantId);
        TenantProfile eval1 = subscriptionService.evaluateTenantTier(tenantId);
        assertEquals(TenantTier.SMB, eval1.tier(), "Should not upgrade on 1st clean window");
        assertEquals(1, subscriptionService.getConsecutiveCleanWindows(tenantId));
        assertEquals(0, subscriptionService.getUpgradeCount(tenantId));

        // Window 2: Process work cleanly again (reaches threshold of 2)
        subscriptionService.recordWorkProcessed(tenantId);
        TenantProfile eval2 = subscriptionService.evaluateTenantTier(tenantId);
        assertEquals(TenantTier.MID_MARKET, eval2.tier(), "Should upgrade to MID_MARKET on 2nd clean window");
        assertEquals(50, subscriptionService.getTenantBurstLimit(tenantId));
        assertEquals(TierSource.ADAPTIVE_TUNING, eval2.tierSource());
        assertEquals(1, subscriptionService.getUpgradeCount(tenantId));
        assertEquals(0, subscriptionService.getConsecutiveCleanWindows(tenantId));

        // Window 3 & 4: Another cycle of sustained clean work
        subscriptionService.recordWorkProcessed(tenantId);
        subscriptionService.evaluateTenantTier(tenantId);
        subscriptionService.recordWorkProcessed(tenantId);
        TenantProfile eval4 = subscriptionService.evaluateTenantTier(tenantId);
        assertEquals(TenantTier.ENTERPRISE, eval4.tier(), "Should step up to ENTERPRISE");
        assertEquals(90, subscriptionService.getTenantBurstLimit(tenantId));
        assertEquals(2, subscriptionService.getUpgradeCount(tenantId));
    }

    @Test
    @DisplayName("Adaptive Tuning: 429 throttle immediately downgrades and oscillation damping freezes upgrades")
    void testAdaptiveAutoDowngradeAndOscillationDampingLock() {
        String tenantId = "tenant-damping-test";
        when(graphClient.discoverTenantProfile(tenantId)).thenReturn(new TenantProfile(
                tenantId,
                "Volatile Corp",
                TenantTier.ENTERPRISE,
                10000,
                TenantTier.ENTERPRISE.getDailyRuBudget(),
                TenantTier.ENTERPRISE.getDefaultBurstLimitRUs(),
                "SPE_E5",
                TierSource.GRAPH_SKU_DISCOVERY,
                false,
                0,
                0
        ));

        subscriptionService.discoverAndRegister(tenantId);
        assertEquals(TenantTier.ENTERPRISE, subscriptionService.getTenantProfile(tenantId).tier());
        assertEquals(90, subscriptionService.getTenantBurstLimit(tenantId));

        // Trigger HTTP 429 Throttle
        subscriptionService.setDampingWindowMs(10000); // 10s damping lock
        subscriptionService.record429ThrottleEvent(tenantId);

        TenantProfile downgraded = subscriptionService.getTenantProfile(tenantId);
        assertEquals(TenantTier.MID_MARKET, downgraded.tier(), "Must immediately step down to MID_MARKET");
        assertEquals(50, subscriptionService.getTenantBurstLimit(tenantId));
        assertEquals(1, subscriptionService.getDowngradeCount(tenantId));
        assertEquals(0, subscriptionService.getConsecutiveCleanWindows(tenantId));

        // During damping window, even with clean processing and multiple evaluations, UPGRADE IS FROZEN
        subscriptionService.recordWorkProcessed(tenantId);
        subscriptionService.evaluateTenantTier(tenantId);
        subscriptionService.recordWorkProcessed(tenantId);
        TenantProfile evalDuringFreeze = subscriptionService.evaluateTenantTier(tenantId);
        assertEquals(TenantTier.MID_MARKET, evalDuringFreeze.tier(), "Upgrade must be frozen during damping window");
        assertEquals(0, subscriptionService.getUpgradeCount(tenantId));

        // Expire damping window via lock reset
        subscriptionService.resetDampingLock(tenantId);
        // Clean window 1 counted
        subscriptionService.recordWorkProcessed(tenantId);
        subscriptionService.evaluateTenantTier(tenantId);
        assertEquals(1, subscriptionService.getConsecutiveCleanWindows(tenantId));

        // Clean window 2 counted -> triggers upgrade back to ENTERPRISE!
        subscriptionService.recordWorkProcessed(tenantId);
        TenantProfile evalAfterExpiry = subscriptionService.evaluateTenantTier(tenantId);
        assertEquals(TenantTier.ENTERPRISE, evalAfterExpiry.tier(), "Should upgrade once damping lock is unlocked");
        assertEquals(90, subscriptionService.getTenantBurstLimit(tenantId));
        assertEquals(1, subscriptionService.getUpgradeCount(tenantId));
    }

    @Test
    @DisplayName("Boundary: SMB tier cannot downgrade below SMB; HYPERSCALE cannot upgrade above HYPERSCALE")
    void testTierBoundaries() {
        String tenantId = "tenant-boundary";
        when(graphClient.discoverTenantProfile(tenantId)).thenReturn(new TenantProfile(
                tenantId,
                "Boundary Corp",
                TenantTier.SMB,
                250,
                TenantTier.SMB.getDailyRuBudget(),
                TenantTier.SMB.getDefaultBurstLimitRUs(),
                "SAFE_BASELINE_FALLBACK",
                TierSource.SAFE_BASELINE_FALLBACK,
                false,
                0,
                0
        ));

        subscriptionService.discoverAndRegister(tenantId);
        assertEquals(TenantTier.SMB, subscriptionService.getTenantProfile(tenantId).tier());

        // 429 at SMB should not underflow or crash
        subscriptionService.record429ThrottleEvent(tenantId);
        assertEquals(TenantTier.SMB, subscriptionService.getTenantProfile(tenantId).tier());
        assertEquals(25, subscriptionService.getTenantBurstLimit(tenantId));

        // Pin to HYPERSCALE and test upgrade boundary
        subscriptionService.setStaticOverride(tenantId, TenantTier.HYPERSCALE);
        assertEquals(TenantTier.HYPERSCALE, subscriptionService.getTenantProfile(tenantId).tier());
        assertEquals(250, subscriptionService.getTenantBurstLimit(tenantId));
    }
}
