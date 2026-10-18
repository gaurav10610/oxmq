package io.oxmq.oxguard.model;

/**
 * Common interface for payloads scoped to a specific tenant.
 * Enables zero-boilerplate extraction of tenantId for OxMQ rate limiter and sentinel plugins.
 */
public interface TenantScopedPayload {
    String tenantId();
}
