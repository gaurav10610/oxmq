package io.cloudbridge.dto;

import java.time.Instant;
import java.util.List;

public record SyncManifest(
    String syncId,
    String source,
    String destination,
    int totalFiles,
    long totalBytes,
    List<FileTransferResult> files,
    Instant completedAt
) {}
