package io.oxmq.example.java17.model;

/**
 * Java 17 record representing the processed invoice result.
 */
public record InvoiceResult(
        String invoiceId,
        String status,
        double tax,
        double totalAmount
) {}
