package io.oxmq;

import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import io.oxmq.client.RedisConnectionManager;
import io.oxmq.metrics.OxmqMetrics;
import io.oxmq.model.Job;
import io.oxmq.model.JobOptions;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.junit.jupiter.api.Assertions.*;

@DisplayName("Reactive Stream Processor Integration Tests")
class ReactiveProcessorIntegrationTest {

    private String queueName;
    private RedisConnectionManager connectionManager;
    private OxmqQueue<String> queue;
    private OxmqWorker<String> worker;
    private SimpleMeterRegistry meterRegistry;
    private OxmqMetrics metrics;

    @BeforeEach
    void setUp() {
        queueName = "reactive-queue-" + UUID.randomUUID();
        connectionManager = new RedisConnectionManager("redis://localhost:6379");
        meterRegistry = new SimpleMeterRegistry();
        metrics = new OxmqMetrics(meterRegistry);
        metrics.setGroupMetricsEnabled(true);

        queue = OxmqQueue.<String>builder()
                .name(queueName)
                .connectionManager(connectionManager)
                .metrics(metrics)
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
        if (meterRegistry != null) {
            meterRegistry.close();
        }
    }

    @Test
    @DisplayName("Reactive processor handles Flux streaming with intermediate progress checkpoints")
    void testReactiveProcessorWithProgressCheckpoints() throws Exception {
        CountDownLatch completedLatch = new CountDownLatch(1);
        List<Integer> observedProgress = Collections.synchronizedList(new ArrayList<>());

        queue.add("reactive-job-1", "test-payload", JobOptions.builder().group("tenant-fintech").build());

        worker = OxmqWorker.<String>builder()
                .queueName(queueName)
                .connectionManager(connectionManager)
                .metrics(metrics)
                .concurrency(1)
                .reactiveStreamsProcessor(job -> {
                    // Emit 25, 50, 75, 100 as progress checkpoints
                    return Flux.just(25, 50, 75, 100)
                            .delayElements(Duration.ofMillis(30));
                })
                .onProgress((job, progress) -> {
                    if (progress instanceof Number n) {
                        observedProgress.add(n.intValue());
                    }
                })
                .onCompleted((job, result) -> {
                    completedLatch.countDown();
                })
                .build();

        worker.start();

        assertTrue(completedLatch.await(5, TimeUnit.SECONDS), "Reactive job should complete within 5 seconds");

        // Verify intermediate progress checkpoints were recorded
        assertFalse(observedProgress.isEmpty(), "Intermediate progress should have been emitted");
        assertTrue(observedProgress.contains(25), "Progress 25 should be recorded");
        assertTrue(observedProgress.contains(100), "Final progress 100 should be recorded");

        // Verify group-tagged telemetry
        double completedCount = 0;
        long deadline = System.currentTimeMillis() + 3000;
        while (System.currentTimeMillis() < deadline) {
            try {
                completedCount = meterRegistry.get("oxmq.jobs.completed")
                        .tag("queue", queueName)
                        .tag("group", "tenant-fintech")
                        .counter()
                        .count();
                if (completedCount >= 1.0) {
                    break;
                }
            } catch (io.micrometer.core.instrument.search.MeterNotFoundException ignored) {
                Thread.sleep(20);
            }
        }
        assertEquals(1.0, completedCount, "Metrics counter should be tagged with tenant-fintech group");
    }

    @Test
    @DisplayName("Reactive processor handles Mono returning job result")
    void testReactiveProcessorWithMono() throws Exception {
        CountDownLatch latch = new CountDownLatch(1);

        String jobId = queue.add("mono-job", "input-data").getId();

        worker = OxmqWorker.<String>builder()
                .queueName(queueName)
                .connectionManager(connectionManager)
                .metrics(metrics)
                .reactiveStreamsProcessor(job -> Mono.just("processed-" + job.getData()))
                .onCompleted((job, result) -> {
                    assertEquals("processed-input-data", result);
                    latch.countDown();
                })
                .build();

        worker.start();

        assertTrue(latch.await(5, TimeUnit.SECONDS), "Mono job should complete");
        Object finishedResult = queue.waitUntilFinished(jobId, Duration.ofSeconds(5));
        assertEquals("processed-input-data", finishedResult);
    }

    @Test
    @DisplayName("Cooperative cancellation of reactive stream when job is cancelled")
    void testReactiveProcessorCancellation() throws Exception {
        CountDownLatch startedLatch = new CountDownLatch(1);
        CountDownLatch cancelledLatch = new CountDownLatch(1);
        AtomicBoolean fluxWasCancelled = new AtomicBoolean(false);

        String jobId = queue.add("cancellable-job", "cancel-payload").getId();

        worker = OxmqWorker.<String>builder()
                .queueName(queueName)
                .connectionManager(connectionManager)
                .reactiveStreamsProcessor(job -> {
                    return Flux.interval(Duration.ofMillis(50))
                            .doOnSubscribe(s -> startedLatch.countDown())
                            .doOnCancel(() -> fluxWasCancelled.set(true))
                            .map(tick -> "tick-" + tick);
                })
                .onFailed((job, err) -> {
                    cancelledLatch.countDown();
                })
                .build();

        worker.start();

        assertTrue(startedLatch.await(5, TimeUnit.SECONDS), "Job should start execution");

        // Cancel job actively running on worker
        boolean cancelled = worker.cancelJob(jobId, "Aborted by admin");
        assertTrue(cancelled, "Worker should successfully cancel running job");

        assertTrue(cancelledLatch.await(5, TimeUnit.SECONDS), "Job failed event should trigger");
        assertTrue(fluxWasCancelled.get(), "Underlying reactive stream subscription must be cancelled cooperatively");
    }
}
