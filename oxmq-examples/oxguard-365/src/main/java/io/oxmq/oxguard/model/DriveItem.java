package io.oxmq.oxguard.model;

/**
 * Representation of a drive item (file or folder) in Microsoft 365.
 */
public record DriveItem(
        String id,
        String driveId,
        String name,
        String path,
        long size,
        String mimeType,
        String webUrl,
        String downloadUrl, // Pre-authenticated SAS URL directly to Azure Blob
        boolean isFolder,
        String eTag,
        String cTag
) {}
