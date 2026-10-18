package io.oxmq.oxguard.model;

/**
 * Payload passed to the parent aggregator node once children complete.
 */
public record FileAggregatePayload(
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
