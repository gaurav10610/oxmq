package io.oxmq.oxguard.mockserver.model;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Model representing an onboarded Microsoft 365 Tenant account with its license tier and RU quotas.
 */
public record TenantProfile(
        String tenantId,
        String organizationName,
        TenantTier tier,
        int seatCount,
        long dailyRuBudget,
        int burstLimitRUsPerSec
) {

    public static final TenantProfile SMB_250 = new TenantProfile(
            "tenant-smb-250",
            "Acme TinyCorp",
            TenantTier.SMB,
            250,
            TenantTier.SMB.getDailyRuBudget(),
            TenantTier.SMB.getDefaultBurstLimitRUs()
    );

    public static final TenantProfile MID_2500 = new TenantProfile(
            "tenant-mid-2500",
            "Contoso Logistics",
            TenantTier.MID_MARKET,
            2500,
            TenantTier.MID_MARKET.getDailyRuBudget(),
            TenantTier.MID_MARKET.getDefaultBurstLimitRUs()
    );

    public static final TenantProfile ENT_10K = new TenantProfile(
            "tenant-ent-10k",
            "Fabrikam Financial",
            TenantTier.ENTERPRISE,
            10000,
            TenantTier.ENTERPRISE.getDailyRuBudget(),
            TenantTier.ENTERPRISE.getDefaultBurstLimitRUs()
    );

    public static final TenantProfile LARGE_35K = new TenantProfile(
            "tenant-large-35k",
            "Northwind Health",
            TenantTier.LARGE_ENTERPRISE,
            35000,
            TenantTier.LARGE_ENTERPRISE.getDailyRuBudget(),
            TenantTier.LARGE_ENTERPRISE.getDefaultBurstLimitRUs()
    );

    public static final TenantProfile GLOBAL_100K = new TenantProfile(
            "tenant-global-100k",
            "MegaCorp Global",
            TenantTier.HYPERSCALE,
            100000,
            TenantTier.HYPERSCALE.getDailyRuBudget(),
            TenantTier.HYPERSCALE.getDefaultBurstLimitRUs()
    );

    public static final List<TenantProfile> ALL_PROFILES = List.of(
            SMB_250, MID_2500, ENT_10K, LARGE_35K, GLOBAL_100K
    );

    public static final Map<String, TenantProfile> BY_ID;

    static {
        Map<String, TenantProfile> map = new LinkedHashMap<>();
        for (TenantProfile profile : ALL_PROFILES) {
            map.put(profile.tenantId(), profile);
        }
        BY_ID = Collections.unmodifiableMap(map);
    }

    public static TenantProfile getByIdOrDefault(String tenantId) {
        if (tenantId == null) {
            return MID_2500;
        }
        return BY_ID.getOrDefault(tenantId, new TenantProfile(
                tenantId,
                "Custom Tenant (" + tenantId + ")",
                TenantTier.MID_MARKET,
                1500,
                TenantTier.MID_MARKET.getDailyRuBudget(),
                TenantTier.MID_MARKET.getDefaultBurstLimitRUs()
        ));
    }
}
