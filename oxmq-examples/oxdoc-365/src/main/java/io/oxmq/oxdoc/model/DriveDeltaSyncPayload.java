package io.oxmq.oxdoc.model;

public record DriveDeltaSyncPayload(
        String tenantId,
        String driveId,
        ScanOrigin scanOrigin,
        String continuationToken
) implements TenantScopedPayload {
    public DriveDeltaSyncPayload(String tenantId, String driveId, ScanOrigin scanOrigin) {
        this(tenantId, driveId, scanOrigin, null);
    }
}
