package io.oxmq.oxguard.model;

/**
 * Access Control Entry parsed from Microsoft Graph permissions.
 */
public record PermissionEntry(
        String id,
        String grantee,
        String role,
        boolean isInherited,
        String inheritedFromId,
        String linkScope
) {}
