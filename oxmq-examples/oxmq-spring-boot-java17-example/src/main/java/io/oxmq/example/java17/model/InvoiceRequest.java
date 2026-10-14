package io.oxmq.example.java17.model;

import java.util.List;

/**
 * Java 17 immutable record representing an incoming invoice processing request.
 */
public record InvoiceRequest(
        String invoiceId,
        String customerEmail,
        double amount,
        List<String> items
) {}
