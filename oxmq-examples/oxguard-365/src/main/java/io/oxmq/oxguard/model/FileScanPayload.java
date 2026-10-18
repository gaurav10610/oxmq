package io.oxmq.oxguard.model;

/**
 * Payload passed to parallel child inspection workers (Pillar 1 and Pillar 2).
 */
public record FileScanPayload(
        String tenantId,
        String driveId,
        String itemId,
        String itemName,
        String itemPath,
        long fileSize,
        String mimeType,
        String downloadUrl,
        ScanOrigin scanOrigin
) {}
