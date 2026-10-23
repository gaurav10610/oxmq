package io.oxmq.oxdoc.model;

/**
 * Persisted state of an indexed document in the content intelligence catalog.
 */
public record IndexedDocumentRecord(
        String fileId,
        String driveId,
        String tenantId,
        String fileName,
        String filePath,
        long fileSize,
        String mimeType,
        AccessScope accessScope,
        boolean hasUniquePermissions,
        IndexingVerdict indexingVerdict,
        String analysisDetails,
        String sha256,
        ScanOrigin scanOrigin,
        long indexedAtTimestamp
) {}
