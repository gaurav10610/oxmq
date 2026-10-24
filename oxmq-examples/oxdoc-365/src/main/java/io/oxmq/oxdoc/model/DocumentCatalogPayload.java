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
        ScanOrigin scanOrigin,
        String traceId
) implements TenantScopedPayload {
    public DocumentCatalogPayload(
            String tenantId,
            String driveId,
            String itemId,
            String itemName,
            String itemPath,
            long fileSize,
            String mimeType,
            String downloadUrl,
            ScanOrigin scanOrigin
    ) {
        this(tenantId, driveId, itemId, itemName, itemPath, fileSize, mimeType, downloadUrl, scanOrigin, null);
    }
}
