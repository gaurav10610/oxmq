package io.oxmq.oxdoc.model;

public record TenantDiscoveryPayload(
        String tenantId,
        String traceId
) implements TenantScopedPayload {
    public TenantDiscoveryPayload(String tenantId) {
        this(tenantId, null);
    }
}
