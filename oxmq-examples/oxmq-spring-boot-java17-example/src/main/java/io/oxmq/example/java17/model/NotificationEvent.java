package io.oxmq.example.java17.model;

/**
 * Java 17 record representing notifications routed to different handlers (email, SMS).
 */
public record NotificationEvent(
        String recipient,
        String subject,
        String content
) {}
