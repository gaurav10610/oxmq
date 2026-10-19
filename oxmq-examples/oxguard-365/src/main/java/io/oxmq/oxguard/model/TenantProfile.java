package io.oxmq.oxguard.model;

/**
 * Model representing an onboarded Microsoft 365 Tenant account dynamically discovered via
 * Microsoft Graph /v1.0/subscribedSkus, user count heuristics, static overrides, or adaptive tuning.
 * Contains no hardcoded static tenant-to-tier mappings.
 */
public record TenantProfile(
        String tenantId,
        String organizationName,
        TenantTier tier,
        int seatCount,
        long dailyRuBudget,
        int burstLimitRUsPerSec,
        String skuPartNumber,
        TierSource tierSource,
        boolean isOverridePinned,
        int upgradeCount,
        int downgradeCount
) {
    public TenantProfile(
            String tenantId,
            String organizationName,
            TenantTier tier,
            int seatCount,
            long dailyRuBudget,
            int burstLimitRUsPerSec,
            String skuPartNumber
    ) {
        this(tenantId, organizationName, tier, seatCount, dailyRuBudget, burstLimitRUsPerSec, skuPartNumber, TierSource.GRAPH_SKU_DISCOVERY, false, 0, 0);
    }

    public TenantProfile(
            String tenantId,
            String organizationName,
            TenantTier tier,
            int seatCount,
            long dailyRuBudget,
            int burstLimitRUsPerSec
    ) {
        this(tenantId, organizationName, tier, seatCount, dailyRuBudget, burstLimitRUsPerSec, "SPE_E5", TierSource.GRAPH_SKU_DISCOVERY, false, 0, 0);
    }

    /**
     * Creates an updated copy of this profile with a new tier, tier source, and transition counts.
     */
    public TenantProfile withTier(TenantTier newTier, TierSource newSource, boolean isPinned, int newUpgradeCount, int newDowngradeCount) {
        return new TenantProfile(
                this.tenantId,
                this.organizationName,
                newTier,
                this.seatCount,
                newTier.getDailyRuBudget(),
                newTier.getDefaultBurstLimitRUs(),
                this.skuPartNumber,
                newSource,
                isPinned,
                newUpgradeCount,
                newDowngradeCount
        );
    }
}
