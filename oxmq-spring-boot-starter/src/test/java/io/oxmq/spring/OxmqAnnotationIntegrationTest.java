package io.oxmq.spring;

import io.oxmq.model.Job;
import io.oxmq.spring.annotation.EnableOxmq;
import io.oxmq.spring.annotation.Header;
import io.oxmq.spring.annotation.OxmqBatchListener;
import io.oxmq.spring.annotation.OxmqListener;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.stereotype.Component;

import static org.assertj.core.api.Assertions.assertThat;

@SpringBootTest(classes = {
        OxmqAnnotationIntegrationTest.TestApp.class,
        OxmqAnnotationIntegrationTest.OrderListener.class,
        OxmqAnnotationIntegrationTest.BatchListener.class,
        OxmqAnnotationIntegrationTest.ClassListener.class
}, properties = {
        "oxmq.redis.uri=redis://localhost:6379",
        "custom.order.queue=test-spring-annotation-orders",
        "custom.batch.queue=test-spring-annotation-batch",
        "custom.order.concurrency=5"
})
public class OxmqAnnotationIntegrationTest {

    @Autowired
    private OxmqTemplate oxmqTemplate;

    @Autowired
    private OrderListener orderListener;

    @Autowired
    private BatchListener batchListener;

    @Autowired
    private ClassListener classListener;

    @AfterEach
    void tearDown() {
        try {
            oxmqTemplate.obliterate("test-spring-annotation-orders");
            oxmqTemplate.obliterate("test-spring-annotation-batch");
            oxmqTemplate.obliterate("test-spring-annotation-multi");
        } catch (Exception ignored) {}
    }

    @Test
    void testAnnotationListenerWithPropertyPlaceholdersAndHeaderInjection() throws Exception {
        orderListener.reset();

        Job<OrderDto> job = oxmqTemplate.send(
                "test-spring-annotation-orders",
                "process-order",
                new OrderDto("ord-12345", 99.95)
        );

        assertThat(job.getId()).isNotNull();

        boolean processed = orderListener.latch.await(5, TimeUnit.SECONDS);
        assertThat(processed).isTrue();
        assertThat(orderListener.receivedPayload.get()).isEqualTo(new OrderDto("ord-12345", 99.95));
        assertThat(orderListener.receivedJobId.get()).isEqualTo(job.getId());
        assertThat(orderListener.receivedJobName.get()).isEqualTo("process-order");
    }

    @Test
    void testBatchAnnotationListenerWithPropertyPlaceholders() throws Exception {
        batchListener.reset();

        for (int i = 1; i <= 5; i++) {
            oxmqTemplate.send("test-spring-annotation-batch", "telemetry-" + i, new TelemetryDto("metric-" + i, i * 10));
        }

        boolean processed = batchListener.latch.await(5, TimeUnit.SECONDS);
        assertThat(processed).isTrue();
        assertThat(batchListener.receivedBatches).isNotEmpty();
    }

    @Test
    void testClassLevelListenerWithHandlerRouting() throws Exception {
        classListener.reset();

        oxmqTemplate.send("test-spring-annotation-multi", "payment", new PaymentEvent("pay-999", 50.0));
        oxmqTemplate.send("test-spring-annotation-multi", "invoice", new InvoiceEvent("inv-888", "Acme Corp"));

        boolean processed = classListener.latch.await(5, TimeUnit.SECONDS);
        assertThat(processed).isTrue();
        assertThat(classListener.receivedPayment.get()).isEqualTo(new PaymentEvent("pay-999", 50.0));
        assertThat(classListener.receivedInvoice.get()).isEqualTo(new InvoiceEvent("inv-888", "Acme Corp"));
    }

    public record OrderDto(String orderId, double amount) {}
    public record TelemetryDto(String metric, int value) {}
    public record PaymentEvent(String paymentId, double amount) {}
    public record InvoiceEvent(String invoiceId, String customer) {}

    @Component
    public static class OrderListener {
        CountDownLatch latch = new CountDownLatch(1);
        AtomicReference<OrderDto> receivedPayload = new AtomicReference<>();
        AtomicReference<String> receivedJobId = new AtomicReference<>();
        AtomicReference<String> receivedJobName = new AtomicReference<>();

        void reset() {
            latch = new CountDownLatch(1);
            receivedPayload.set(null);
            receivedJobId.set(null);
            receivedJobName.set(null);
        }

        @OxmqListener(queue = "${custom.order.queue}", concurrencyString = "${custom.order.concurrency}")
        public String handleOrder(OrderDto order, @Header("jobId") String jobId, @Header("jobName") String jobName) {
            receivedPayload.set(order);
            receivedJobId.set(jobId);
            receivedJobName.set(jobName);
            latch.countDown();
            return "SUCCESS";
        }
    }

    @Component
    public static class BatchListener {
        CountDownLatch latch = new CountDownLatch(1);
        List<List<Job<TelemetryDto>>> receivedBatches = new CopyOnWriteArrayList<>();

        void reset() {
            latch = new CountDownLatch(1);
            receivedBatches.clear();
        }

        @OxmqBatchListener(queue = "${custom.batch.queue}", batchSize = 5)
        public void handleBatch(List<Job<TelemetryDto>> batch) {
            receivedBatches.add(batch);
            latch.countDown();
        }
    }

    @Component
    @OxmqListener(queue = "test-spring-annotation-multi", concurrency = 5)
    public static class ClassListener {
        CountDownLatch latch = new CountDownLatch(2);
        AtomicReference<PaymentEvent> receivedPayment = new AtomicReference<>();
        AtomicReference<InvoiceEvent> receivedInvoice = new AtomicReference<>();

        void reset() {
            latch = new CountDownLatch(2);
            receivedPayment.set(null);
            receivedInvoice.set(null);
        }

        @io.oxmq.spring.annotation.OxmqHandler
        public void handlePayment(PaymentEvent event) {
            receivedPayment.set(event);
            latch.countDown();
        }

        @io.oxmq.spring.annotation.OxmqHandler
        public void handleInvoice(InvoiceEvent event) {
            receivedInvoice.set(event);
            latch.countDown();
        }
    }

    @SpringBootApplication
    @EnableOxmq
    public static class TestApp {
    }
}
