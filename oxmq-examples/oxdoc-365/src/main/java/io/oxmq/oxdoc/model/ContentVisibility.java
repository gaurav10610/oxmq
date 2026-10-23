package io.oxmq.oxdoc.model;

import java.util.List;

/**
 * Resolved content visibility and access scope for a document item.
 */
public record ContentVisibility(
        AccessScope scope,
        boolean hasUniquePermissions,
        List<PermissionEntry> permissions
) {}
