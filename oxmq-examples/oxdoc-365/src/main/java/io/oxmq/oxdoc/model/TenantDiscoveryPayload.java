package io.oxmq.oxdoc.model;

public record TenantDiscoveryPayload(
        String tenantId
) implements TenantScopedPayload {}
