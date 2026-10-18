package io.oxmq.oxguard.model;

import java.util.List;

/**
 * Resolved effective exposure status for a file item.
 */
public record EffectiveExposure(
        ExposureLevel level,
        boolean hasUniquePermissions,
        List<PermissionEntry> permissions
) {}
