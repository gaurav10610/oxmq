package io.oxmq.example.java17;

import io.oxmq.spring.annotation.EnableOxmq;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

/**
 * Showcase application demonstrating 100% annotation-driven OxMQ integration
 * running on Java 17 LTS (and Java 21 LTS with Virtual Threads).
 */
@SpringBootApplication
@EnableOxmq
public class OxmqJava17Application {

    public static void main(String[] args) {
        SpringApplication.run(OxmqJava17Application.class, args);
    }
}
