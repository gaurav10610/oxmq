package io.oxmq.oxdoc.model;

/**
 * Common interface for payloads scoped to a specific tenant.
 * Enables zero-boilerplate extraction of tenantId for OxMQ rate limiter and sentinel plugins.
 */
public interface TenantScopedPayload {
    String tenantId();

    default ScanOrigin scanOrigin() {
        return ScanOrigin.BACKWARD;
    }
}
