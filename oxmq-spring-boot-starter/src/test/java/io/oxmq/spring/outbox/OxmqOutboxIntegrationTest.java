package io.oxmq.spring.outbox;

import io.oxmq.OxmqQueue;
import io.oxmq.OxmqWorker;
import io.oxmq.client.RedisConnectionManager;
import io.oxmq.model.Job;
import io.oxmq.model.JobOptions;
import io.oxmq.serializer.JacksonJobSerializer;
import io.oxmq.spring.OxmqTemplate;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.*;

@DisplayName("Transactional Outbox Pattern Integration Tests")
class OxmqOutboxIntegrationTest {

    private String queueName;
    private RedisConnectionManager connectionManager;
    private InMemoryOxmqOutboxRepository repository;
    private JacksonJobSerializer serializer;
    private OxmqTemplate template;
    private DefaultOxmqOutboxService outboxService;
    private OxmqOutboxPublisher publisher;
    private OxmqWorker<String> worker;

    record PaymentData(String orderId, double amount) {}

    @BeforeEach
    void setUp() {
        queueName = "outbox-test-" + UUID.randomUUID();
        connectionManager = new RedisConnectionManager("redis://localhost:6379");
        repository = new InMemoryOxmqOutboxRepository();
        serializer = new JacksonJobSerializer();

        io.lettuce.core.RedisClient redisClient = io.lettuce.core.RedisClient.create("redis://localhost:6379");
        io.oxmq.lua.LuaScriptManager scriptManager = new io.oxmq.lua.LuaScriptManager();
        io.oxmq.metrics.OxmqMetrics metrics = new io.oxmq.metrics.OxmqMetrics(null);

        template = new OxmqTemplate(redisClient, connectionManager, scriptManager, serializer, metrics, null);
        outboxService = new DefaultOxmqOutboxService(repository, serializer);
        publisher = new OxmqOutboxPublisher(repository, template, serializer, 10, 50, Duration.ofMinutes(5));
    }

    @AfterEach
    void tearDown() {
        if (publisher != null) {
            publisher.stop();
        }
        if (worker != null) {
            try { worker.close(); } catch (Exception ignored) {}
        }
        if (template != null) {
            try { template.obliterate(queueName); } catch (Exception ignored) {}
        }
        if (connectionManager != null) {
            try { connectionManager.close(); } catch (Exception ignored) {}
        }
    }

    @Test
    @DisplayName("Transactional Outbox persists record, publishes to Redis, and worker consumes with group metadata")
    void testOutboxEnqueuesAndPublishesToRedis() throws Exception {
        PaymentData payment = new PaymentData("order-101", 99.95);

        // 1. Transactional enqueue into outbox
        String outboxId = outboxService.enqueueGroup(queueName, "process-payment", "tenant-corp",
                payment, JobOptions.builder().build());

        assertNotNull(outboxId);
        assertEquals(1, repository.findPending(10).size());
        assertEquals(OxmqOutboxRecord.Status.PENDING, repository.findById(outboxId).get().getStatus());

        // 2. Setup worker to receive the job from Redis
        CountDownLatch latch = new CountDownLatch(1);
        AtomicReference<Job<String>> receivedJobRef = new AtomicReference<>();

        worker = OxmqWorker.<String>builder()
                .queueName(queueName)
                .connectionManager(connectionManager)
                .processor(job -> {
                    receivedJobRef.set(job);
                    latch.countDown();
                    return "processed";
                })
                .build();
        worker.start();

        // 3. Trigger outbox publisher drain cycle
        int published = publisher.publishCycle();
        assertEquals(1, published, "1 record should have been published to Redis");

        // 4. Verify outbox record status is updated to PUBLISHED
        OxmqOutboxRecord recordAfter = repository.findById(outboxId).get();
        assertEquals(OxmqOutboxRecord.Status.PUBLISHED, recordAfter.getStatus());
        assertNotNull(recordAfter.getPublishedAt());
        assertEquals(0, repository.findPending(10).size(), "No pending records should remain");

        // 5. Worker receives and processes the job
        assertTrue(latch.await(5, TimeUnit.SECONDS), "Worker should receive job from Redis");
        Job<String> job = receivedJobRef.get();
        assertNotNull(job);
        assertEquals("process-payment", job.getName());
        assertEquals("tenant-corp", job.getGroupKey());
        assertTrue(job.getData().contains("order-101"));
        assertTrue(job.getData().contains("99.95"));
    }

    @Test
    @DisplayName("Outbox records with errors are marked failed and attempts are incremented")
    void testOutboxErrorHandling() {
        OxmqOutboxRecord record = new OxmqOutboxRecord("invalid-queue", "task", "{}", "{}", "tenant-err");
        repository.save(record);

        repository.markFailed(record.getId(), "Connection refused");
        OxmqOutboxRecord updated = repository.findById(record.getId()).get();
        assertEquals(1, updated.getAttempts());
        assertEquals("Connection refused", updated.getLastError());
        assertEquals(OxmqOutboxRecord.Status.PENDING, updated.getStatus());

        // Increment up to max attempts
        for (int i = 0; i < 9; i++) {
            repository.markFailed(record.getId(), "Error " + i);
        }
        assertEquals(OxmqOutboxRecord.Status.FAILED, repository.findById(record.getId()).get().getStatus());
    }

    @Test
    @DisplayName("Outbox purges published records older than configured retention")
    void testOutboxPurgeRetention() {
        OxmqOutboxRecord record = new OxmqOutboxRecord(queueName, "task", "{}", "{}", "grp");
        repository.save(record);
        repository.markPublished(record.getId());

        // Backdate publishedAt
        record.setPublishedAt(java.time.Instant.now().minus(Duration.ofHours(2)));

        long purged = repository.purgePublishedOlderThan(Duration.ofHours(1));
        assertEquals(1, purged);
        assertTrue(repository.findById(record.getId()).isEmpty());
    }
}
