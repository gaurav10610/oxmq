package io.oxmq.oxguard.model;

/**
 * Microsoft 365 Paid License Tiers based on official Microsoft Learn throttling guidance.
 */
public enum TenantTier {
    SMB("Small Business (0 - 1,000 Seats)", 1_200_000L, 25),
    MID_MARKET("Mid-Market Growth (1,001 - 5,000 Seats)", 2_400_000L, 50),
    ENTERPRISE("Enterprise E3/E5 (5,001 - 15,000 Seats)", 3_600_000L, 90),
    LARGE_ENTERPRISE("Large Enterprise (15,001 - 50,000 Seats)", 4_800_000L, 150),
    HYPERSCALE("Hyperscale Global (50,000+ Seats)", 6_000_000L, 250);

    private final String displayName;
    private final long dailyRuBudget;
    private final int defaultBurstLimitRUs;

    TenantTier(String displayName, long dailyRuBudget, int defaultBurstLimitRUs) {
        this.displayName = displayName;
        this.dailyRuBudget = dailyRuBudget;
        this.defaultBurstLimitRUs = defaultBurstLimitRUs;
    }

    /**
     * Resolves the official Microsoft 365 capacity tier dynamically from total licensed user seats.
     */
    public static TenantTier fromSeatCount(int seats) {
        if (seats <= 1000) return SMB;
        if (seats <= 5000) return MID_MARKET;
        if (seats <= 15000) return ENTERPRISE;
        if (seats <= 50000) return LARGE_ENTERPRISE;
        return HYPERSCALE;
    }

    /**
     * Resolves the tier dynamically from Microsoft Graph subscribedSkus skuPartNumber and seat count.
     */
    public static TenantTier fromSku(String skuPartNumber, int seats) {
        if (skuPartNumber != null) {
            String upper = skuPartNumber.toUpperCase();
            if ((upper.contains("SMB") || upper.contains("BUSINESS")) && seats <= 1000) {
                return SMB;
            }
        }
        return fromSeatCount(seats);
    }

    public String getDisplayName() {
        return displayName;
    }

    public long getDailyRuBudget() {
        return dailyRuBudget;
    }

    public int getDefaultBurstLimitRUs() {
        return defaultBurstLimitRUs;
    }
}
