package io.cloudbridge.dto;

public record FileTransferResult(
    String fileId,
    String path,
    long bytesTransferred,
    String sha256,
    String status,
    long durationMs
) {}
