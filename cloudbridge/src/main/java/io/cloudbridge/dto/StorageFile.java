package io.cloudbridge.dto;

public record StorageFile(String path, long sizeBytes, String sha256, String mimeType) {}
