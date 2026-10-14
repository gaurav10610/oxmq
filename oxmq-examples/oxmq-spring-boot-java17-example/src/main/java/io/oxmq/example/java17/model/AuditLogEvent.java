package io.oxmq.example.java17.model;

/**
 * Java 17 record representing an audit log event for high-throughput batching.
 */
public record AuditLogEvent(
        String eventId,
        String action,
        String principal,
        long timestamp
) {}
