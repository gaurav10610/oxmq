package io.oxmq.oxdoc;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;
import org.springframework.scheduling.annotation.EnableScheduling;

/**
 * OxDoc 365: Enterprise Content Intelligence & Document AI Indexing Showcase Application.
 * Powered by Java 17 LTS and OxMQ.
 */
@SpringBootApplication
@EnableScheduling
public class OxDocApplication {

    private static final Logger log = LoggerFactory.getLogger(OxDocApplication.class);

    public static void main(String[] args) {
        SpringApplication.run(OxDocApplication.class, args);
    }

    @Bean
    public CommandLineRunner initMessage() {
        return args -> {
            log.info("==========================================================================================");
            log.info("  📄  OXDOC 365 :: ENTERPRISE CONTENT INTELLIGENCE & DOCUMENT AI (JAVA 17 LTS + OXMQ)     ");
            log.info("==========================================================================================");
            log.info("  - Dashboard UI:               http://localhost:8080                                      ");
            log.info("  - Metrics REST API:           http://localhost:8080/api/v1/dashboard/metrics             ");
            log.info("  - Live SSE Event Stream:      http://localhost:8080/api/v1/dashboard/sse                 ");
            log.info("  - Microsoft Graph Webhook:    http://localhost:8080/api/v1/webhooks/graph                ");
            log.info("==========================================================================================");
        };
    }
}
