package io.oxmq;

import io.lettuce.core.RedisClient;
import io.oxmq.client.QueueKeys;
import io.oxmq.client.RedisConnectionManager;
import io.oxmq.model.FlowJob;
import io.oxmq.model.Job;
import io.oxmq.model.JobOptions;
import io.oxmq.model.JobState;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.*;

/**
 * Comprehensive integration tests verifying native context isolation and multi-service namespacing.
 * Confirms that multiple independent services can share a single Redis instance with zero key
 * collisions, zero worker cross-talk, and full workflow isolation.
 */
class ContextIsolationIntegrationTest {

    private RedisClient redisClient;
    private RedisConnectionManager connectionManager;
    private final List<OxmqQueue<?>> queuesToClean = new ArrayList<>();
    private final List<OxmqWorker<?>> workersToClean = new ArrayList<>();
    private final List<QueueEvents> eventsToClean = new ArrayList<>();

    @BeforeEach
    void setUp() {
        String redisUri = System.getProperty("oxmq.redis.uri", "redis://localhost:6379");
        redisClient = RedisClient.create(redisUri);
        connectionManager = new RedisConnectionManager(redisClient);
    }

    @AfterEach
    void tearDown() {
        for (OxmqWorker<?> worker : workersToClean) {
            try { worker.close(); } catch (Exception ignored) {}
        }
        for (QueueEvents events : eventsToClean) {
            try { events.close(); } catch (Exception ignored) {}
        }
        for (OxmqQueue<?> queue : queuesToClean) {
            try { queue.obliterate(); queue.close(); } catch (Exception ignored) {}
        }
        if (connectionManager != null) {
            connectionManager.close();
        }
        if (redisClient != null) {
            redisClient.shutdown();
        }
    }

    @Test
    @DisplayName("Verify QueueKeys correctly isolates Redis key paths and hash tags")
    void testQueueKeysIsolation() {
        QueueKeys billingKeys = new QueueKeys("billing", "orders");
        QueueKeys shippingKeys = new QueueKeys("shipping", "orders");
        QueueKeys defaultKeys = new QueueKeys("orders");

        assertThat(billingKeys.getPrefix()).isEqualTo("billing");
        assertThat(billingKeys.getQueueName()).isEqualTo("orders");
        assertThat(billingKeys.toKey("")).isEqualTo("billing:orders:");
        assertThat(billingKeys.toKey("wait")).isEqualTo("billing:orders:wait");
        assertThat(billingKeys.toKey("active")).isEqualTo("billing:orders:active");
        assertThat(billingKeys.toKey("id")).isEqualTo("billing:orders:id");

        assertThat(shippingKeys.getPrefix()).isEqualTo("shipping");
        assertThat(shippingKeys.getQueueName()).isEqualTo("orders");
        assertThat(shippingKeys.toKey("")).isEqualTo("shipping:orders:");
        assertThat(shippingKeys.toKey("wait")).isEqualTo("shipping:orders:wait");

        // BullMQ default fallback
        assertThat(defaultKeys.getPrefix()).isEqualTo("bull");
        assertThat(defaultKeys.toKey("wait")).isEqualTo("bull:orders:wait");

        // Verify keys do not overlap
        assertThat(billingKeys.toKey("wait")).isNotEqualTo(shippingKeys.toKey("wait"));
        assertThat(billingKeys.toKey("wait")).isNotEqualTo(defaultKeys.toKey("wait"));
    }

    @Test
    @DisplayName("Worker Cross-Talk Prevention: Same queue name across two service namespaces")
    void testWorkerCrossTalkPrevention() throws Exception {
        String queueName = "orders-pipeline-" + System.currentTimeMillis();
        String prefixAlpha = "service-alpha";
        String prefixBeta = "service-beta";

        OxmqQueue<String> queueAlpha = OxmqQueue.<String>builder()
                .name(queueName)
                .prefix(prefixAlpha)
                .redisClient(redisClient)
                .payloadClass(String.class)
                .build();
        queuesToClean.add(queueAlpha);

        OxmqQueue<String> queueBeta = OxmqQueue.<String>builder()
                .name(queueName)
                .prefix(prefixBeta)
                .redisClient(redisClient)
                .payloadClass(String.class)
                .build();
        queuesToClean.add(queueBeta);

        List<String> receivedAlpha = new CopyOnWriteArrayList<>();
        List<String> receivedBeta = new CopyOnWriteArrayList<>();
        CountDownLatch alphaLatch = new CountDownLatch(2);
        CountDownLatch betaLatch = new CountDownLatch(2);

        OxmqWorker<String> workerAlpha = OxmqWorker.<String>builder()
                .queueName(queueName)
                .prefix(prefixAlpha)
                .redisClient(redisClient)
                .processor(job -> {
                    receivedAlpha.add(job.getData());
                    alphaLatch.countDown();
                    return "ok";
                })
                .build();
        workersToClean.add(workerAlpha);
        workerAlpha.start();

        OxmqWorker<String> workerBeta = OxmqWorker.<String>builder()
                .queueName(queueName)
                .prefix(prefixBeta)
                .redisClient(redisClient)
                .processor(job -> {
                    receivedBeta.add(job.getData());
                    betaLatch.countDown();
                    return "ok";
                })
                .build();
        workersToClean.add(workerBeta);
        workerBeta.start();

        // Enqueue to Alpha
        queueAlpha.add("order-job", "alpha-payload-1");
        queueAlpha.add("order-job", "alpha-payload-2");

        // Enqueue to Beta
        queueBeta.add("order-job", "beta-payload-1");
        queueBeta.add("order-job", "beta-payload-2");

        assertTrue(alphaLatch.await(10, TimeUnit.SECONDS), "Worker Alpha should process its 2 jobs");
        assertTrue(betaLatch.await(10, TimeUnit.SECONDS), "Worker Beta should process its 2 jobs");

        // Verify complete isolation
        assertThat(receivedAlpha).containsExactlyInAnyOrder("alpha-payload-1", "alpha-payload-2");
        assertThat(receivedBeta).containsExactlyInAnyOrder("beta-payload-1", "beta-payload-2");
        assertThat(receivedAlpha).doesNotContain("beta-payload-1", "beta-payload-2");
        assertThat(receivedBeta).doesNotContain("alpha-payload-1", "alpha-payload-2");
    }

    @Test
    @DisplayName("Deduplication and Custom JobId Isolation: Same jobId allowed in different namespaces")
    void testDeduplicationIsolation() {
        String queueName = "dedup-test-" + System.currentTimeMillis();

        OxmqQueue<String> queueTenant1 = OxmqQueue.<String>builder()
                .name(queueName)
                .prefix("tenant-1")
                .redisClient(redisClient)
                .payloadClass(String.class)
                .build();
        queuesToClean.add(queueTenant1);

        OxmqQueue<String> queueTenant2 = OxmqQueue.<String>builder()
                .name(queueName)
                .prefix("tenant-2")
                .redisClient(redisClient)
                .payloadClass(String.class)
                .build();
        queuesToClean.add(queueTenant2);

        String sharedJobId = "invoice-99999";
        JobOptions opts = JobOptions.builder().jobId(sharedJobId).build();

        Job<String> job1 = queueTenant1.add("invoice", "tenant1-data", opts);
        Job<String> job2 = queueTenant2.add("invoice", "tenant2-data", opts);

        assertNotNull(job1);
        assertNotNull(job2);
        assertEquals(sharedJobId, job1.getId());
        assertEquals(sharedJobId, job2.getId());

        // Verify retrieval per tenant
        Job<String> fetched1 = queueTenant1.getJob(sharedJobId);
        Job<String> fetched2 = queueTenant2.getJob(sharedJobId);

        assertThat(fetched1).isNotNull();
        assertThat(fetched2).isNotNull();
        assertThat(fetched1.getData()).isEqualTo("tenant1-data");
        assertThat(fetched2.getData()).isEqualTo("tenant2-data");
    }

    @Test
    @DisplayName("QueueEvents Isolation: Events only delivered to matching namespace")
    void testQueueEventsIsolation() throws Exception {
        String queueName = "events-test-" + System.currentTimeMillis();

        OxmqQueue<String> queueServiceX = OxmqQueue.<String>builder()
                .name(queueName)
                .prefix("service-x")
                .redisClient(redisClient)
                .payloadClass(String.class)
                .build();
        queuesToClean.add(queueServiceX);

        OxmqQueue<String> queueServiceY = OxmqQueue.<String>builder()
                .name(queueName)
                .prefix("service-y")
                .redisClient(redisClient)
                .payloadClass(String.class)
                .build();
        queuesToClean.add(queueServiceY);

        List<String> eventsX = new CopyOnWriteArrayList<>();
        List<String> eventsY = new CopyOnWriteArrayList<>();

        QueueEvents qeX = QueueEvents.builder()
                .queueName(queueName)
                .prefix("service-x")
                .redisClient(redisClient)
                .build();
        eventsToClean.add(qeX);

        QueueEvents qeY = QueueEvents.builder()
                .queueName(queueName)
                .prefix("service-y")
                .redisClient(redisClient)
                .build();
        eventsToClean.add(qeY);

        qeX.onCompleted((jobId, result) -> eventsX.add(jobId));
        qeY.onCompleted((jobId, result) -> eventsY.add(jobId));

        qeX.start();
        qeY.start();

        CountDownLatch latchX = new CountDownLatch(1);
        OxmqWorker<String> workerX = OxmqWorker.<String>builder()
                .queueName(queueName)
                .prefix("service-x")
                .redisClient(redisClient)
                .processor(job -> {
                    latchX.countDown();
                    return "done";
                })
                .build();
        workersToClean.add(workerX);
        workerX.start();

        Job<String> enqueued = queueServiceX.add("task", "payload-for-x");

        assertTrue(latchX.await(10, TimeUnit.SECONDS), "Worker X should finish");
        Thread.sleep(600); // Allow event propagation via Redis Pub/Sub

        assertThat(eventsX).contains(enqueued.getId());
        assertThat(eventsY).isEmpty();
    }

    @Test
    @DisplayName("FlowProducer DAG Isolation across Service Namespaces")
    void testFlowProducerNamespacing() throws Exception {
        String parentQueue = "flow-parent-" + System.currentTimeMillis();
        String childQueue = "flow-child-" + System.currentTimeMillis();

        OxmqClient billingClient = OxmqClient.builder()
                .prefix("billing")
                .redisClient(redisClient)
                .build();

        OxmqClient shippingClient = OxmqClient.builder()
                .prefix("shipping")
                .redisClient(redisClient)
                .build();

        OxmqQueue<String> bParentQ = billingClient.queue(parentQueue, String.class);
        OxmqQueue<String> bChildQ = billingClient.queue(childQueue, String.class);
        queuesToClean.add(bParentQ);
        queuesToClean.add(bChildQ);

        OxmqQueue<String> sParentQ = shippingClient.queue(parentQueue, String.class);
        OxmqQueue<String> sChildQ = shippingClient.queue(childQueue, String.class);
        queuesToClean.add(sParentQ);
        queuesToClean.add(sChildQ);

        // Add DAG in billing
        FlowJob<String> childJob = FlowJob.of(childQueue, "bill-child", "data-child");
        FlowJob<String> parentJob = FlowJob.of(parentQueue, "bill-parent", "data-parent");
        parentJob.addChild(childJob);

        String billingParentId = billingClient.flowProducer().add(parentJob);
        assertNotNull(billingParentId);

        // Verify billing queue has the job in WAITING_CHILDREN
        Job<String> billingJob = bParentQ.getJob(billingParentId);
        assertNotNull(billingJob);
        assertThat(bParentQ.count(JobState.WAITING_CHILDREN)).isEqualTo(1L);

        // Verify shipping queue of the same name does NOT have this job
        Job<String> shippingJob = sParentQ.getJob(billingParentId);
        assertNull(shippingJob, "Shipping namespace must not see Billing DAG job");
        assertThat(sParentQ.count(JobState.WAITING_CHILDREN)).isEqualTo(0L);
    }

    @Test
    @DisplayName("OxmqClient Service-Level Entrypoint and forService Fluent Switching")
    void testOxmqClientAndForService() {
        OxmqClient rootClient = OxmqClient.builder()
                .prefix("core-service")
                .redisClient(redisClient)
                .build();

        assertThat(rootClient.getPrefix()).isEqualTo("core-service");

        OxmqQueue<String> myQueue = rootClient.queue("notifications", String.class);
        queuesToClean.add(myQueue);
        assertThat(myQueue.getNamespace()).isEqualTo("core-service");

        // Fluent switch to another service
        OxmqClient analyticsClient = rootClient.forService("analytics-service");
        assertThat(analyticsClient.getPrefix()).isEqualTo("analytics-service");

        OxmqQueue<String> analyticsQueue = analyticsClient.queue("notifications", String.class);
        queuesToClean.add(analyticsQueue);
        assertThat(analyticsQueue.getNamespace()).isEqualTo("analytics-service");
    }

    @Test
    @DisplayName("Prefix Resolution Precedence: env/system property fallback")
    void testPrefixResolutionPrecedence() {
        // Explicit prefix always wins
        assertThat(QueueKeys.resolvePrefix("custom-prefix")).isEqualTo("custom-prefix");

        // Null or blank falls back to default "bull" when no env or system property set
        assertThat(QueueKeys.resolvePrefix(null)).isEqualTo("bull");
        assertThat(QueueKeys.resolvePrefix("")).isEqualTo("bull");
        assertThat(QueueKeys.resolvePrefix("   ")).isEqualTo("bull");

        // System property override
        System.setProperty("oxmq.prefix", "sys-prop-namespace");
        try {
            assertThat(QueueKeys.resolvePrefix(null)).isEqualTo("sys-prop-namespace");
            assertThat(QueueKeys.resolvePrefix("")).isEqualTo("sys-prop-namespace");
            // Explicit still overrides system property
            assertThat(QueueKeys.resolvePrefix("explicit")).isEqualTo("explicit");
        } finally {
            System.clearProperty("oxmq.prefix");
        }
    }
}
