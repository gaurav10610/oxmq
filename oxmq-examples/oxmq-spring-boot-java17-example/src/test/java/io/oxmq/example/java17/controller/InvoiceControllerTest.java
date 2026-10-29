package io.oxmq.example.java17.controller;

import io.oxmq.example.java17.OxmqJava17Application;
import io.oxmq.example.java17.model.InvoiceRequest;
import io.oxmq.example.java17.model.NotificationEvent;
import io.oxmq.spring.OxmqTemplate;
import java.util.List;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.http.MediaType;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.web.servlet.MockMvc;

import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

@SpringBootTest(classes = OxmqJava17Application.class)
@AutoConfigureMockMvc
@DirtiesContext(classMode = DirtiesContext.ClassMode.AFTER_CLASS)
public class InvoiceControllerTest {

    @Autowired
    private MockMvc mockMvc;

    @Autowired
    private OxmqTemplate oxmqTemplate;

    @AfterEach
    void tearDown() {
        try {
            oxmqTemplate.resume("java17-invoices");
        } catch (Exception ignored) {}
        try {
            oxmqTemplate.obliterate("java17-invoices");
            oxmqTemplate.obliterate("java17-audit-logs");
            oxmqTemplate.obliterate("java17-notifications");
        } catch (Exception ignored) {}
    }

    @Test
    void testSubmitInvoiceHttpEndpoint() throws Exception {
        String json = """
                {
                    "invoiceId": "http-inv-101",
                    "customerEmail": "customer@web.com",
                    "amount": 500.00,
                    "items": ["SKU-1", "SKU-2"]
                }
                """;

        mockMvc.perform(post("/api/invoices")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(json))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.status").value("QUEUED"))
                .andExpect(jsonPath("$.invoiceId").value("http-inv-101"))
                .andExpect(jsonPath("$.jobId").isNotEmpty())
                .andExpect(jsonPath("$.traceId").isNotEmpty());
    }

    @Test
    void testSendNotificationHttpEndpoint() throws Exception {
        String json = """
                {
                    "recipient": "dev@oxmq.io",
                    "subject": "System Alert",
                    "content": "All systems operational"
                }
                """;

        mockMvc.perform(post("/api/notifications")
                        .param("type", "email")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(json))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.jobId").isNotEmpty())
                .andExpect(jsonPath("$.channel").value("email"));
    }

    @Test
    void testPauseAndResumeQueueEndpoints() throws Exception {
        mockMvc.perform(post("/api/queues/java17-invoices/pause"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.state").value("PAUSED"));

        mockMvc.perform(post("/api/queues/java17-invoices/resume"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.state").value("ACTIVE"));
    }
}
