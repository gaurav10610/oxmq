package io.oxmq.oxdoc.model;

/**
 * Payload passed to the parent document catalog aggregator node once children complete.
 */
public record DocumentCatalogPayload(
        String tenantId,
        String driveId,
        String itemId,
        String itemName,
        String itemPath,
        long fileSize,
        String mimeType,
        String downloadUrl,
        ScanOrigin scanOrigin
) implements TenantScopedPayload {}
