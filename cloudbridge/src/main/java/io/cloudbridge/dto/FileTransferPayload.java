package io.cloudbridge.dto;

public record FileTransferPayload(
    String fileId,
    String path,
    long sizeBytes,
    String sha256,
    String source,
    String destination,
    String parentJobId
) {}
