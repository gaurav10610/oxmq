package io.oxmq.oxdoc.model;

public record DriveDeltaSyncPayload(
        String tenantId,
        String driveId,
        ScanOrigin scanOrigin,
        String continuationToken,
        String traceId
) implements TenantScopedPayload {
    public DriveDeltaSyncPayload(String tenantId, String driveId, ScanOrigin scanOrigin) {
        this(tenantId, driveId, scanOrigin, null, null);
    }

    public DriveDeltaSyncPayload(String tenantId, String driveId, ScanOrigin scanOrigin, String continuationToken) {
        this(tenantId, driveId, scanOrigin, continuationToken, null);
    }
}
