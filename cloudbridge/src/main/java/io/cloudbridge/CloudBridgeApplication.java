package io.cloudbridge;

import io.oxmq.spring.annotation.EnableOxmq;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication
@EnableOxmq
public class CloudBridgeApplication {
    private static final Logger log = LoggerFactory.getLogger(CloudBridgeApplication.class);

    public static void main(String[] args) {
        log.info("Starting CloudBridge (Multi-Cloud Sync Pipeline powered by OxMQ)...");
        SpringApplication.run(CloudBridgeApplication.class, args);
        log.info("==========================================================================");
        log.info(" CloudBridge Dashboard running at: http://localhost:8080");
        log.info(" REST API endpoint for sync:       http://localhost:8080/api/sync");
        log.info(" Queue metrics endpoint:          http://localhost:8080/api/queues");
        log.info("==========================================================================");
    }
}
