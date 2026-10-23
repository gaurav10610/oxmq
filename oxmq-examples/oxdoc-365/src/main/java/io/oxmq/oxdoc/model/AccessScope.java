package io.oxmq.oxdoc.model;

/**
 * Normalized 5-tier access scopes for enterprise content intelligence and knowledge assets.
 */
public enum AccessScope {
    PUBLIC_ANONYMOUS,     // Accessible via anonymous public link without credentials (e.g. public knowledge base)
    EXTERNAL_GUEST,       // Shared with external vendors/guest email addresses
    ORGANIZATION_WIDE,    // Shared across all authenticated users in the tenant
    INTERNAL_RESTRICTED,  // Shared only with named internal users/department groups
    PRIVATE               // Accessible exclusively by the owner
}
