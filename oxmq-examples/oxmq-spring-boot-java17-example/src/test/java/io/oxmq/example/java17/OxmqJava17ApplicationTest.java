package io.oxmq.example.java17;

import io.oxmq.example.java17.listener.AuditBatchListener;
import io.oxmq.example.java17.listener.InvoiceProcessingListener;
import io.oxmq.example.java17.listener.MultiEventNotificationListener;
import io.oxmq.example.java17.model.AuditLogEvent;
import io.oxmq.example.java17.model.InvoiceRequest;
import io.oxmq.example.java17.model.InvoiceResult;
import io.oxmq.example.java17.model.NotificationEvent;
import io.oxmq.model.Job;
import io.oxmq.spring.OxmqTemplate;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.test.context.SpringBootTest;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.NONE)
public class OxmqJava17ApplicationTest {

    @Autowired
    private OxmqTemplate oxmqTemplate;

    @Autowired
    private InvoiceProcessingListener invoiceListener;

    @Autowired
    private AuditBatchListener auditBatchListener;

    @Autowired
    private MultiEventNotificationListener notificationListener;

    @Value("${app.queues.invoices:java17-invoices}")
    private String invoiceQueue;

    @Value("${app.queues.audit:java17-audit-logs}")
    private String auditQueue;

    @Value("${app.queues.notifications:java17-notifications}")
    private String notificationQueue;

    @BeforeEach
    void setUp() {
        invoiceListener.clear();
        auditBatchListener.clear();
        notificationListener.clear();
    }

    @AfterEach
    void tearDown() {
        try {
            oxmqTemplate.obliterate(invoiceQueue);
            oxmqTemplate.obliterate(auditQueue);
            oxmqTemplate.obliterate(notificationQueue);
        } catch (Exception ignored) {}
    }

    @Test
    void testMethodLevelInvoiceListener() {
        InvoiceRequest request = new InvoiceRequest(
                "inv-2026-001",
                "billing@enterprise.com",
                1000.00,
                List.of("ITEM-A", "ITEM-B")
        );

        Job<InvoiceRequest> job = oxmqTemplate.send(invoiceQueue, "process-invoice", request);
        assertThat(job.getId()).isNotNull();

        await().atMost(5, TimeUnit.SECONDS).untilAsserted(() -> {
            InvoiceResult result = invoiceListener.getProcessedInvoices().get("inv-2026-001");
            assertThat(result).isNotNull();
            assertThat(result.status()).isEqualTo("PROCESSED");
            assertThat(result.tax()).isEqualTo(180.00);
            assertThat(result.totalAmount()).isEqualTo(1180.00);
        });
    }

    @Test
    void testBatchLevelAuditListener() {
        for (int i = 1; i <= 10; i++) {
            AuditLogEvent event = new AuditLogEvent(
                    "evt-" + i,
                    "USER_LOGIN",
                    "user-" + i + "@domain.com",
                    System.currentTimeMillis()
            );
            oxmqTemplate.send(auditQueue, "audit-login", event);
        }

        await().atMost(5, TimeUnit.SECONDS).untilAsserted(() -> {
            assertThat(auditBatchListener.getReceivedBatches()).isNotEmpty();
            int totalProcessed = auditBatchListener.getReceivedBatches().stream()
                    .mapToInt(List::size)
                    .sum();
            assertThat(totalProcessed).isGreaterThanOrEqualTo(5);
        });
    }

    @Test
    void testClassLevelMultiHandlerRouting() {
        NotificationEvent emailNotice = new NotificationEvent(
                "alice@domain.com",
                "Invoice Ready",
                "Your invoice inv-2026-001 is ready."
        );
        NotificationEvent smsNotice = new NotificationEvent(
                "+1-555-0199",
                "OTP Code",
                "Your verification code is 482910."
        );

        oxmqTemplate.send(notificationQueue, "email", emailNotice);
        oxmqTemplate.send(notificationQueue, "sms", smsNotice);

        await().atMost(5, TimeUnit.SECONDS).untilAsserted(() -> {
            assertThat(notificationListener.getDeliveredEmails()).contains(emailNotice);
            assertThat(notificationListener.getDeliveredSms()).contains(smsNotice);
        });
    }

    @Test
    void testQueuePauseResumeAndJobLookup() {
        oxmqTemplate.pause(invoiceQueue);

        InvoiceRequest request = new InvoiceRequest(
                "inv-paused-001",
                "paused@enterprise.com",
                250.00,
                List.of("ITEM-P")
        );
        Job<InvoiceRequest> job = oxmqTemplate.send(invoiceQueue, "process-invoice", request);

        Job<Object> retrieved = oxmqTemplate.getJob(invoiceQueue, job.getId());
        assertThat(retrieved).isNotNull();
        assertThat(retrieved.getId()).isEqualTo(job.getId());

        oxmqTemplate.resume(invoiceQueue);

        await().atMost(5, TimeUnit.SECONDS).untilAsserted(() -> {
            assertThat(invoiceListener.getProcessedInvoices()).containsKey("inv-paused-001");
        });
    }
}
