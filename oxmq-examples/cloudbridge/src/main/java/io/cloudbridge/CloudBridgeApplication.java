package io.cloudbridge;

import io.oxmq.spring.annotation.EnableOxmq;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;

@SpringBootApplication
@EnableOxmq
public class CloudBridgeApplication {
    private static final Logger log = LoggerFactory.getLogger(CloudBridgeApplication.class);

    public static void main(String[] args) {
        loadDotEnvIfPresent();
        log.info("Starting CloudBridge (Multi-Cloud Sync Pipeline powered by OxMQ)...");
        SpringApplication.run(CloudBridgeApplication.class, args);
        log.info("==========================================================================");
        log.info(" CloudBridge Dashboard running at: http://localhost:8080");
        log.info(" REST API endpoint for sync:       http://localhost:8080/api/sync");
        log.info(" Queue metrics endpoint:          http://localhost:8080/api/queues");
        log.info("==========================================================================");
    }

    private static void loadDotEnvIfPresent() {
        File[] candidates = new File[] {
            new File(".env"),
            new File("oxmq-examples/cloudbridge/.env"),
            new File("cloudbridge/.env"),
            new File("../.env"),
            new File("../../.env")
        };
        for (File file : candidates) {
            if (file.exists() && file.isFile()) {
                try (BufferedReader reader = new BufferedReader(new FileReader(file))) {
                    String line;
                    while ((line = reader.readLine()) != null) {
                        line = line.trim();
                        if (line.isEmpty() || line.startsWith("#") || !line.contains("=")) continue;
                        int eq = line.indexOf('=');
                        String key = line.substring(0, eq).trim();
                        String val = line.substring(eq + 1).trim();
                        if ((val.startsWith("\"") && val.endsWith("\"")) || (val.startsWith("'") && val.endsWith("'"))) {
                            val = val.substring(1, val.length() - 1);
                        }
                        if (System.getProperty(key) == null && System.getenv(key) == null) {
                            System.setProperty(key, val);
                        }
                    }
                } catch (Exception ignored) {}
                break;
            }
        }
    }
}
