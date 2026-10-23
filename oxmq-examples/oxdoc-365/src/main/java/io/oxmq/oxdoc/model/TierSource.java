package io.oxmq.oxdoc.model;

/**
 * Origin and resolution method of an onboarded tenant's active license tier and capacity limits.
 */
public enum TierSource {
    /** Pinned via application configuration or admin API */
    STATIC_OVERRIDE,

    /** Discovered directly via Microsoft Graph GET /v1.0/subscribedSkus */
    GRAPH_SKU_DISCOVERY,

    /** Discovered via heuristic user count (GET /v1.0/users?$count=true) when /subscribedSkus is restricted */
    USER_COUNT_HEURISTIC,

    /** Automatically escalated or de-escalated by the Adaptive Capacity Controller */
    ADAPTIVE_TUNING,

    /** Safe fail-safe baseline assigned when all discovery mechanisms are inaccessible */
    SAFE_BASELINE_FALLBACK
}
