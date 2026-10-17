package io.oxmq;

import io.lettuce.core.RedisClient;
import io.oxmq.client.QueueKeys;
import io.oxmq.model.Job;
import io.oxmq.model.JobOptions;
import io.oxmq.model.JobState;
import io.oxmq.model.KeepJobs;
import java.time.Duration;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

public class MemoryOptimizationIntegrationTest {

    private RedisClient redisClient;
    private String queueName;
    private OxmqQueue<String> queue;

    @BeforeEach
    void setUp() {
        String redisUri = System.getProperty("oxmq.redis.uri", "redis://localhost:6379");
        redisClient = RedisClient.create(redisUri);
        queueName = "mem-opt-" + System.currentTimeMillis();
    }

    @AfterEach
    void tearDown() {
        if (queue != null) {
            try {
                queue.obliterate();
                queue.close();
            } catch (Exception ignored) {}
        }
        if (redisClient != null) {
            redisClient.shutdown();
        }
    }

    @Test
    void testDefaultJobOptionsRetentionInheritance() {
        JobOptions defaultOpts = JobOptions.builder()
                .removeOnComplete(KeepJobs.of(500, Duration.ofDays(3)))
                .removeOnFail(KeepJobs.of(2000, Duration.ofDays(14)))
                .build();

        queue = OxmqQueue.<String>builder()
                .name(queueName)
                .redisClient(redisClient)
                .payloadClass(String.class)
                .defaultJobOptions(defaultOpts)
                .build();

        Job<String> job = queue.add("test-job", "hello-world");
        assertThat(job).isNotNull();
        assertThat(job.getOpts()).isNotNull();
        assertThat(job.getOpts().getKeepCompletedJobs()).isNotNull();
        assertThat(job.getOpts().getKeepCompletedJobs().getCount()).isEqualTo(500);
        assertThat(job.getOpts().getKeepCompletedJobs().getAgeSeconds()).isEqualTo(Duration.ofDays(3).toSeconds());
        assertThat(job.getOpts().getKeepFailedJobs()).isNotNull();
        assertThat(job.getOpts().getKeepFailedJobs().getCount()).isEqualTo(2000);
    }

    @Test
    void testMaxLenEventsConfiguration() {
        queue = OxmqQueue.<String>builder()
                .name(queueName)
                .redisClient(redisClient)
                .payloadClass(String.class)
                .maxLenEvents(1234)
                .build();

        QueueKeys keys = new QueueKeys(null, queueName);
        var sync = redisClient.connect().sync();
        String maxEvents = sync.hget(keys.toKey("meta"), "opts.maxLenEvents");
        assertThat(maxEvents).isEqualTo("1234");
    }

    @Test
    void testLazyChildrenValuesResolution() {
        Job<String> job = new Job<>("job-1", "parent", "data", null);
        AtomicBoolean supplierInvoked = new AtomicBoolean(false);

        job.setChildrenValuesSupplier(() -> {
            supplierInvoked.set(true);
            return Map.of("child-1", "result-1");
        });

        // Supplier should not be invoked until getChildrenValues() is called
        assertThat(supplierInvoked.get()).isFalse();

        Map<String, Object> values = job.getChildrenValues();
        assertThat(supplierInvoked.get()).isTrue();
        assertThat(values).containsEntry("child-1", "result-1");

        // Subsequent calls return cached value without invoking supplier again
        supplierInvoked.set(false);
        Map<String, Object> secondValues = job.getChildrenValues();
        assertThat(supplierInvoked.get()).isFalse();
        assertThat(secondValues).isSameAs(values);
    }

    @Test
    void testStackTraceTruncationOnFailure() throws Exception {
        queue = OxmqQueue.<String>builder()
                .name(queueName)
                .redisClient(redisClient)
                .payloadClass(String.class)
                .build();

        CountDownLatch failLatch = new CountDownLatch(1);
        AtomicReference<String> failedJobId = new AtomicReference<>();

        // Create an exception with a huge message/trace (> 15 KB)
        StringBuilder hugeMsg = new StringBuilder();
        for (int i = 0; i < 2000; i++) {
            hugeMsg.append("Error step ").append(i).append(": stack trace frame detail overflow test.\n");
        }
        RuntimeException deepException = new RuntimeException(hugeMsg.toString());

        OxmqWorker<String> worker = OxmqWorker.<String>builder()
                .queueName(queueName)
                .redisClient(redisClient)
                .payloadClass(String.class)
                .processor(j -> {
                    failedJobId.set(j.getId());
                    failLatch.countDown();
                    throw deepException;
                })
                .build();

        worker.start();
        try {
            queue.add("failing-job", "crash-test", JobOptions.builder().attempts(1).build());
            assertThat(failLatch.await(10, TimeUnit.SECONDS)).isTrue();

            // Give worker time to complete moveToFinished
            Thread.sleep(1000);

            var sync = redisClient.connect().sync();
            QueueKeys keys = new QueueKeys(null, queueName);
            String failedReason = sync.hget(keys.toJobKey(failedJobId.get()), "failedReason");

            assertThat(failedReason).isNotNull();
            assertThat(failedReason).contains("[... stack trace truncated for memory optimization ...]");
            assertThat(failedReason.length()).isLessThanOrEqualTo(8192 + 100);

        } finally {
            worker.close();
        }
    }
}
