package io.oxmq;

import io.oxmq.client.RedisConnectionManager;
import io.oxmq.relay.OxmqRelay;
import io.oxmq.relay.RelayMessage;
import io.oxmq.relay.RelaySubscription;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;

@DisplayName("OxmqRelay Distributed Pub/Sub & Control Plane Integration Tests")
class OxmqRelayIntegrationTest {

    private RedisConnectionManager connectionManager;
    private OxmqRelay relayA;
    private OxmqRelay relayB;
    private String prefix;

    public record OrderStatusUpdate(String orderId, String status) {}

    @BeforeEach
    void setUp() {
        prefix = "relay-test-" + UUID.randomUUID();
        connectionManager = new RedisConnectionManager("redis://localhost:6379");
        relayA = OxmqRelay.builder()
                .connectionManager(connectionManager)
                .prefix(prefix)
                .nodeId("node-alpha")
                .build();
        relayB = OxmqRelay.builder()
                .connectionManager(connectionManager)
                .prefix(prefix)
                .nodeId("node-beta")
                .build();
    }

    @AfterEach
    void tearDown() {
        if (relayA != null) {
            try { relayA.close(); } catch (Exception ignored) {}
        }
        if (relayB != null) {
            try { relayB.close(); } catch (Exception ignored) {}
        }
        if (connectionManager != null) {
            try { connectionManager.close(); } catch (Exception ignored) {}
        }
    }

    @Test
    @DisplayName("Topic pattern matching correctly routes exact, single-segment (*), and multi-segment (>) topics")
    void testTopicPatternMatchingRules() {
        // Exact
        assertTrue(OxmqRelay.matchesPattern("orders.status", "orders.status"));
        assertFalse(OxmqRelay.matchesPattern("orders.status", "orders.different"));

        // Single segment wildcard (*)
        assertTrue(OxmqRelay.matchesPattern("orders.*.status", "orders.123.status"));
        assertFalse(OxmqRelay.matchesPattern("orders.*.status", "orders.123.456.status"));

        // Multi segment trailing wildcard (>)
        assertTrue(OxmqRelay.matchesPattern("jobs.>", "jobs.exports.csv"));
        assertTrue(OxmqRelay.matchesPattern("jobs.>", "jobs.sync"));
        assertFalse(OxmqRelay.matchesPattern("jobs.>", "orders.sync"));
    }

    @Test
    @DisplayName("Inter-node pub/sub delivers messages across nodes matching wildcard subscriptions")
    void testInterNodePublishSubscribe() throws Exception {
        CountDownLatch latch = new CountDownLatch(2);
        List<RelayMessage> received = new CopyOnWriteArrayList<>();

        // relayB subscribes to wildcard pattern
        RelaySubscription sub = relayB.subscribe("orders.*.status", msg -> {
            received.add(msg);
            latch.countDown();
        });

        // Give subscription a moment to connect
        Thread.sleep(100);

        // relayA publishes messages
        relayA.publish("orders.101.status", new OrderStatusUpdate("101", "PAID"));
        relayA.publish("orders.202.status", new OrderStatusUpdate("202", "SHIPPED"));
        relayA.publish("orders.303.created", new OrderStatusUpdate("303", "IGNORED")); // Non-matching

        assertTrue(latch.await(5, TimeUnit.SECONDS), "Both matching order status messages should be received");
        assertEquals(2, received.size());

        OrderStatusUpdate update1 = received.get(0).getPayload(OrderStatusUpdate.class);
        assertEquals("101", update1.orderId());
        assertEquals("PAID", update1.status());

        OrderStatusUpdate update2 = received.get(1).getPayload(OrderStatusUpdate.class);
        assertEquals("202", update2.orderId());
        assertEquals("SHIPPED", update2.status());

        sub.unsubscribe();
        assertFalse(sub.isActive());
    }

    @Test
    @DisplayName("Retained messages are cached and immediately delivered to newly connecting subscribers")
    void testRetainedMessageDelivery() throws Exception {
        // relayA publishes a retained configuration/state update
        relayA.publish("config.features.darkmode", "enabled", Duration.ofSeconds(10));

        CountDownLatch retainedLatch = new CountDownLatch(1);
        List<RelayMessage> received = new CopyOnWriteArrayList<>();

        // relayB subscribes AFTER the message was already published
        RelaySubscription sub = relayB.subscribe("config.features.*", msg -> {
            received.add(msg);
            retainedLatch.countDown();
        });

        assertTrue(retainedLatch.await(5, TimeUnit.SECONDS), "Newly subscribed node should immediately receive retained message");
        assertEquals(1, received.size());
        assertTrue(received.get(0).isRetained());
        assertEquals("enabled", received.get(0).getPayload(String.class));

        sub.unsubscribe();
    }

    @Test
    @DisplayName("Active nodes register heartbeats and are discoverable across the cluster")
    void testNodeHeartbeatsAndDiscovery() {
        List<String> nodes = relayA.getActiveNodes();
        assertNotNull(nodes);
        assertTrue(nodes.contains("node-alpha"), "Active nodes list must contain node-alpha");
        assertTrue(nodes.contains("node-beta"), "Active nodes list must contain node-beta");
    }
}
