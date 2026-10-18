package io.oxmq.oxguard.model;

public record DriveDeltaSyncPayload(
        String tenantId,
        String driveId,
        ScanOrigin scanOrigin
) {}
