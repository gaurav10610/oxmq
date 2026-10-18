package io.oxmq.oxguard.model;

/**
 * Persisted state of a security-scanned file in the DSPM inventory.
 */
public record ScannedFileRecord(
        String fileId,
        String driveId,
        String tenantId,
        String fileName,
        String filePath,
        long fileSize,
        String mimeType,
        ExposureLevel exposureLevel,
        boolean hasUniquePermissions,
        ThreatVerdict threatVerdict,
        String threatDetails,
        String sha256,
        ScanOrigin scanOrigin,
        long scannedAtTimestamp
) {}
