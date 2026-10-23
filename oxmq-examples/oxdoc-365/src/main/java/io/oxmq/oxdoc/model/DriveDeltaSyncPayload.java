package io.oxmq.oxdoc.model;

public record DriveDeltaSyncPayload(
        String tenantId,
        String driveId,
        ScanOrigin scanOrigin
) implements TenantScopedPayload {}
