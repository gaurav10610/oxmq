package io.oxmq;

import io.lettuce.core.api.sync.RedisCommands;
import io.oxmq.client.QueueKeys;
import io.oxmq.client.RedisConnectionManager;
import io.oxmq.interceptor.InterceptorDecision;
import io.oxmq.interceptor.JobInterceptor;
import io.oxmq.model.GroupOptions;
import io.oxmq.model.Job;
import io.oxmq.model.JobOptions;
import io.oxmq.model.JobState;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.*;

class QueueObliterateAndGroupLeakIntegrationTest {

    private String queueName;
    private RedisConnectionManager connectionManager;
    private OxmqQueue<String> queue;
    private OxmqWorker<String> worker;

    @BeforeEach
    void setUp() {
        queueName = "audit-fix-" + UUID.randomUUID();
        connectionManager = new RedisConnectionManager("redis://localhost:6379");
        queue = OxmqQueue.<String>builder()
                .name(queueName)
                .connectionManager(connectionManager)
                .build();
    }

    @AfterEach
    void tearDown() {
        if (worker != null) {
            try { worker.close(); } catch (Exception ignored) {}
        }
        if (queue != null) {
            try { queue.obliterate(); } catch (Exception ignored) {}
            try { queue.close(); } catch (Exception ignored) {}
        }
        if (connectionManager != null) {
            try { connectionManager.close(); } catch (Exception ignored) {}
        }
    }

    @Test
    @DisplayName("BUG-02: obliterate() on unpaused queue with jobs pauses queue and completely purges all jobs")
    void testObliterateUnpausedQueue() {
        // Enqueue 20 jobs without pausing
        for (int i = 0; i < 20; i++) {
            queue.add("job-" + i, "data-" + i);
        }

        assertThat(queue.count(JobState.WAITING)).isEqualTo(20);

        // Obliterate without pre-pausing manually
        queue.obliterate();

        // Must be completely emptied
        assertThat(queue.count(JobState.WAITING)).isEqualTo(0);
        assertThat(queue.getJobCounts()).allSatisfy((state, count) -> assertThat(count).isEqualTo(0));
    }

    @Test
    @DisplayName("BUG-10: add() with group options and delayMs > 0 prioritizes delay and lands in DELAYED state")
    void testAddGroupJobWithDelay() {
        JobOptions opts = JobOptions.builder()
                .group(GroupOptions.builder().id("tenant-delayed-group").build())
                .delayMs(5000)
                .build();

        Job<String> job = queue.add("delayed-group-job", "payload", opts);

        assertNotNull(job);
        assertThat(queue.getJobState(job.getId())).isEqualTo(JobState.DELAYED);
        assertThat(queue.count(JobState.DELAYED)).isEqualTo(1);
    }

    @Test
    @DisplayName("BUG-01: Interceptor returning delay does not leak group active concurrency counter")
    void testGroupActiveCounterDoesNotLeakOnInterceptorDelay() throws Exception {
        String groupId = "tenant-interceptor-group";
        QueueKeys keys = new QueueKeys(queueName);
        RedisCommands<String, String> sync = connectionManager.getCommandConnection().sync();

        AtomicBoolean delayedOnce = new AtomicBoolean(false);
        CountDownLatch completeLatch = new CountDownLatch(1);

        // Interceptor delays the job once before processing
        JobInterceptor delayInterceptor = new JobInterceptor() {
            @Override
            public InterceptorDecision beforeProcess(Job<?> job) {
                if (delayedOnce.compareAndSet(false, true)) {
                    return InterceptorDecision.delay(100, true);
                }
                return InterceptorDecision.proceed();
            }
        };

        worker = OxmqWorker.<String>builder()
                .queueName(queueName)
                .connectionManager(connectionManager)
                .groupConcurrency(1)
                .interceptor(delayInterceptor)
                .processor(job -> {
                    completeLatch.countDown();
                    return "OK";
                })
                .build();

        JobOptions opts = JobOptions.builder()
                .group(GroupOptions.builder().id(groupId).build())
                .build();

        queue.add("group-job", "test-data", opts);
        worker.start();

        assertTrue(completeLatch.await(5, TimeUnit.SECONDS), "Job should eventually complete after delay");
        worker.close();

        // Verify that group active counter is 0 and not leaked
        String activeGroupKey = keys.toKey("group:" + groupId + ":active");
        String activeCountStr = sync.get(activeGroupKey);
        int activeCount = (activeCountStr != null) ? Integer.parseInt(activeCountStr) : 0;
        assertEquals(0, activeCount, "Group active counter must be exactly 0 after execution");
    }
}
