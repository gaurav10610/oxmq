package io.oxmq.spring.actuator;

import io.lettuce.core.RedisClient;
import io.oxmq.spring.postprocessor.OxmqListenerAnnotationBeanPostProcessor;
import org.springframework.boot.actuate.health.Health;
import org.springframework.boot.actuate.health.HealthIndicator;

/**
 * Actuator HealthIndicator reporting OxMQ Redis connectivity and active listener status.
 */
public class OxmqHealthIndicator implements HealthIndicator {

    private final RedisClient redisClient;
    private final OxmqListenerAnnotationBeanPostProcessor postProcessor;
    private final String prefix;

    public OxmqHealthIndicator(RedisClient redisClient, OxmqListenerAnnotationBeanPostProcessor postProcessor) {
        this(redisClient, postProcessor, null);
    }

    public OxmqHealthIndicator(RedisClient redisClient, OxmqListenerAnnotationBeanPostProcessor postProcessor, String prefix) {
        this.redisClient = redisClient;
        this.postProcessor = postProcessor;
        this.prefix = prefix;
    }

    @Override
    public Health health() {
        try (var conn = redisClient.connect()) {
            String ping = conn.sync().ping();
            int workerCount = postProcessor != null ? postProcessor.getRegisteredWorkers().size() : 0;
            boolean running = postProcessor != null && postProcessor.isRunning();

            if ("PONG".equalsIgnoreCase(ping)) {
                return Health.up()
                        .withDetail("redis", "CONNECTED")
                        .withDetail("namespace", prefix != null && !prefix.isBlank() ? prefix : "bull")
                        .withDetail("registeredWorkers", workerCount)
                        .withDetail("workersRunning", running)
                        .build();
            } else {
                return Health.down().withDetail("redis", "Unexpected PING response: " + ping).build();
            }
        } catch (Exception e) {
            return Health.down(e).withDetail("redis", "DISCONNECTED").build();
        }
    }
}
