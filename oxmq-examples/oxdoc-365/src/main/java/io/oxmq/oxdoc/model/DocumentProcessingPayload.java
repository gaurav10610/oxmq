package io.oxmq.oxdoc.model;

/**
 * Payload passed to parallel document processing child workers (extraction and categorization).
 */
public record DocumentProcessingPayload(
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
    public DocumentProcessingPayload(
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
