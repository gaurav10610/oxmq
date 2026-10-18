package io.oxmq.oxguard.model;

/**
 * Normalized 5-tier exposure levels for enterprise data assets.
 */
public enum ExposureLevel {
    PUBLIC_ANONYMOUS,     // Accessible via anonymous public link without credentials
    EXTERNAL_GUEST,       // Shared with external vendors/guest email addresses
    ORGANIZATION_WIDE,    // Shared with all authenticated users in the tenant
    INTERNAL_RESTRICTED,  // Shared only with named internal users/groups
    PRIVATE               // Accessible exclusively by the owner
}
