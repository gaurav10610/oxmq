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
        ScanOrigin scanOrigin
) implements TenantScopedPayload {}
