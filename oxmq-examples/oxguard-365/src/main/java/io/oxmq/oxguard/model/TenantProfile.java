package io.oxmq.oxguard.model;

/**
 * Model representing an onboarded Microsoft 365 Tenant account dynamically discovered via
 * Microsoft Graph /v1.0/subscribedSkus and /v1.0/organization.
 * Contains no static tenant-to-tier mappings.
 */
public record TenantProfile(
        String tenantId,
        String organizationName,
        TenantTier tier,
        int seatCount,
        long dailyRuBudget,
        int burstLimitRUsPerSec,
        String skuPartNumber
) {
    public TenantProfile(
            String tenantId,
            String organizationName,
            TenantTier tier,
            int seatCount,
            long dailyRuBudget,
            int burstLimitRUsPerSec
    ) {
        this(tenantId, organizationName, tier, seatCount, dailyRuBudget, burstLimitRUsPerSec, "SPE_E5");
    }
}
