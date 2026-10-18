package io.oxmq.oxguard.mockserver;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.concurrent.CountDownLatch;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.context.annotation.Bean;

/**
 * Standalone Spring Boot Application for Ultra-Realistic Microsoft Office 365 Mock Server.
 * Runs on port 8081 independently from OxGuard 365.
 */
@SpringBootApplication
public class MockOffice365Application {

    private static final Logger log = LoggerFactory.getLogger(MockOffice365Application.class);
    private static final CountDownLatch keepAliveLatch = new CountDownLatch(1);

    public static void main(String[] args) {
        ConfigurableApplicationContext context = SpringApplication.run(MockOffice365Application.class, args);
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            log.info("Shutdown hook triggered for MockOffice365Application");
            keepAliveLatch.countDown();
        }));

        log.info("Mock Office 365 Standalone Service running. Press Ctrl+C or kill to exit.");
        try {
            keepAliveLatch.await();
        } catch (InterruptedException ignored) {
            Thread.currentThread().interrupt();
        }
    }

    @Bean
    @ConditionalOnMissingBean
    public ObjectMapper objectMapper() {
        return new ObjectMapper();
    }
}
