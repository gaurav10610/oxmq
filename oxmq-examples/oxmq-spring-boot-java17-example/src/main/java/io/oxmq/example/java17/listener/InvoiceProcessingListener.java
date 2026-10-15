package io.oxmq.example.java17.listener;

import io.oxmq.example.java17.model.InvoiceRequest;
import io.oxmq.example.java17.model.InvoiceResult;
import io.oxmq.spring.annotation.Header;
import io.oxmq.spring.annotation.OxmqListener;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

/**
 * Demonstrates method-level {@link OxmqListener} with:
 * - Property placeholder resolution for queue name and concurrency
 * - Automated parameter binding with Java 17 records
 * - {@link Header} parameter injection for metadata
 */
@Component
public class InvoiceProcessingListener {

    private static final Logger log = LoggerFactory.getLogger(InvoiceProcessingListener.class);

    private final Map<String, InvoiceResult> processedInvoices = new ConcurrentHashMap<>();

    @OxmqListener(
            queue = "${app.queues.invoices:java17-invoices}",
            concurrencyString = "${app.concurrency.invoices:8}"
    )
    public InvoiceResult processInvoice(
            InvoiceRequest request,
            @Header("jobId") String jobId,
            @Header("jobName") String jobName,
            @Header("timestamp") long timestamp
    ) {
        log.info("Processing invoice [{}] (jobId={}, name={}, timestamp={}) for customer: {}",
                request.invoiceId(), jobId, jobName, timestamp, request.customerEmail());

        if (request.invoiceId().contains("postpone") && !processedInvoices.containsKey(request.invoiceId() + "-postponed")) {
            processedInvoices.put(request.invoiceId() + "-postponed", new InvoiceResult(request.invoiceId(), "POSTPONED", 0, 0));
            log.info("Invoice [{}] requested postponement, throwing DelayedException for 2 seconds (skipAttempt: true)", request.invoiceId());
            throw new io.oxmq.exception.DelayedException(java.time.Duration.ofSeconds(2));
        }

        double tax = Math.round(request.amount() * 0.18 * 100.0) / 100.0;
        double total = request.amount() + tax;

        InvoiceResult result = new InvoiceResult(request.invoiceId(), "PROCESSED", tax, total);
        processedInvoices.put(request.invoiceId(), result);

        log.info("Invoice [{}] processed successfully: total=${}", request.invoiceId(), total);
        return result;
    }

    public Map<String, InvoiceResult> getProcessedInvoices() {
        return processedInvoices;
    }

    public void clear() {
        processedInvoices.clear();
    }
}
