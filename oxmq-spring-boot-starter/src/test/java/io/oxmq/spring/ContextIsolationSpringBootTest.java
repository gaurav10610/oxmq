package io.oxmq.spring;

import io.oxmq.model.Job;
import io.oxmq.spring.actuator.OxmqHealthIndicator;
import io.oxmq.spring.annotation.EnableOxmq;
import io.oxmq.spring.annotation.OxmqListener;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.actuate.health.Health;
import org.springframework.boot.actuate.health.Status;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;
import org.springframework.boot.autoconfigure.AutoConfigurations;
import org.springframework.stereotype.Component;

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertTrue;

@SpringBootTest(classes = {
        ContextIsolationSpringBootTest.TestApp.class,
        ContextIsolationSpringBootTest.DefaultInheritedListener.class,
        ContextIsolationSpringBootTest.ExplicitPrefixListener.class
}, properties = {
        "spring.application.name=billing-service",
        "oxmq.redis.uri=redis://localhost:6379",
        "billing.queue=invoices-queue"
})
public class ContextIsolationSpringBootTest {

    @Autowired
    private OxmqTemplate oxmqTemplate;

    @Autowired
    private OxmqHealthIndicator healthIndicator;

    @Autowired
    private DefaultInheritedListener inheritedListener;

    @Autowired
    private ExplicitPrefixListener explicitListener;

    @AfterEach
    void tearDown() {
        try {
            oxmqTemplate.obliterate("invoices-queue");
            oxmqTemplate.toService("shipping-service").getQueue("invoices-queue").obliterate();
            oxmqTemplate.toService("security-service").getQueue("audit-queue").obliterate();
        } catch (Exception ignored) {}
    }

    @Test
    @DisplayName("Verify Spring Boot auto-inherits spring.application.name as default namespace")
    void testSpringApplicationNameInheritance() throws Exception {
        assertThat(oxmqTemplate.getDefaultPrefix()).isEqualTo("billing-service");

        Health health = healthIndicator.health();
        assertThat(health.getStatus()).isEqualTo(Status.UP);
        assertThat(health.getDetails().get("namespace")).isEqualTo("billing-service");

        // Send job to billing-service's invoices-queue
        inheritedListener.reset();
        oxmqTemplate.send("invoices-queue", "INV-1001");

        assertTrue(inheritedListener.await(10, TimeUnit.SECONDS), "Inherited listener should process the job");
        assertThat(inheritedListener.getReceived()).containsExactly("INV-1001");

        // Now send a job to shipping-service's invoices-queue via toService()
        // The billing-service listener must NOT receive it!
        oxmqTemplate.toService("shipping-service").send("invoices-queue", "SHIP-2002");
        Thread.sleep(500);

        assertThat(inheritedListener.getReceived())
                .as("Billing worker must never receive jobs from shipping-service namespace")
                .doesNotContain("SHIP-2002");
    }

    @Test
    @DisplayName("Verify explicit @OxmqListener(prefix = '...') overrides service-level default")
    void testExplicitPrefixListener() throws Exception {
        explicitListener.reset();

        // Send to security-service namespace
        oxmqTemplate.toService("security-service").send("audit-queue", "SECURITY-EVENT-99");

        assertTrue(explicitListener.await(10, TimeUnit.SECONDS), "Explicit listener should receive job");
        assertThat(explicitListener.getReceived()).containsExactly("SECURITY-EVENT-99");
    }

    @Test
    @DisplayName("Verify precedence: oxmq.prefix property overrides spring.application.name")
    void testOxmqPrefixPrecedenceOverAppName() {
        new ApplicationContextRunner()
                .withConfiguration(AutoConfigurations.of(OxmqAutoConfiguration.class))
                .withPropertyValues(
                        "spring.application.name=app-name-service",
                        "oxmq.prefix=explicit-oxmq-prefix"
                )
                .run(context -> {
                    OxmqProperties props = context.getBean(OxmqProperties.class);
                    String resolved = props.resolvePrefix(context.getEnvironment());
                    assertThat(resolved).isEqualTo("explicit-oxmq-prefix");

                    OxmqTemplate template = context.getBean(OxmqTemplate.class);
                    assertThat(template.getDefaultPrefix()).isEqualTo("explicit-oxmq-prefix");
                });
    }

    @SpringBootApplication
    @EnableOxmq
    static class TestApp {}

    @Component
    static class DefaultInheritedListener {
        private final List<String> received = new CopyOnWriteArrayList<>();
        private CountDownLatch latch = new CountDownLatch(1);

        @OxmqListener(queue = "${billing.queue}")
        public void onInvoice(String invoice) {
            received.add(invoice);
            latch.countDown();
        }

        public void reset() {
            received.clear();
            latch = new CountDownLatch(1);
        }

        public boolean await(long timeout, TimeUnit unit) throws InterruptedException {
            return latch.await(timeout, unit);
        }

        public List<String> getReceived() {
            return received;
        }
    }

    @Component
    static class ExplicitPrefixListener {
        private final List<String> received = new CopyOnWriteArrayList<>();
        private CountDownLatch latch = new CountDownLatch(1);

        @OxmqListener(queue = "audit-queue", prefix = "security-service")
        public void onAudit(String log) {
            received.add(log);
            latch.countDown();
        }

        public void reset() {
            received.clear();
            latch = new CountDownLatch(1);
        }

        public boolean await(long timeout, TimeUnit unit) throws InterruptedException {
            return latch.await(timeout, unit);
        }

        public List<String> getReceived() {
            return received;
        }
    }
}
