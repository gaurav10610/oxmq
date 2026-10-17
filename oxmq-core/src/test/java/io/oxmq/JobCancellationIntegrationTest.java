package io.oxmq;

import io.oxmq.client.RedisConnectionManager;
import io.oxmq.exception.JobCancelledException;
import io.oxmq.model.Job;
import io.oxmq.model.JobOptions;
import io.oxmq.model.JobState;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;

public class JobCancellationIntegrationTest {

    private static final String REDIS_URI = "redis://localhost:6379";
    private RedisConnectionManager connectionManager;
    private List<Queue<?>> queuesToClean;
    private List<Worker<?>> workersToStop;

    @BeforeEach
    void setUp() {
        connectionManager = new RedisConnectionManager(REDIS_URI);
        queuesToClean = new ArrayList<>();
        workersToStop = new ArrayList<>();
    }

    @AfterEach
    void tearDown() {
        for (Worker<?> worker : workersToStop) {
            try {
                worker.close();
            } catch (Exception ignored) {}
        }
        for (Queue<?> queue : queuesToClean) {
            try {
                queue.obliterate();
                queue.close();
            } catch (Exception ignored) {}
        }
        if (connectionManager != null) {
            try {
                connectionManager.close();
            } catch (Exception ignored) {}
        }
    }

    @Test
    @DisplayName("Single in-flight job cancellation interrupts execution and marks job unrecoverably failed without retry")
    void testSingleJobCancellationWithInterruption() throws Exception {
        String queueName = "cancel-test-" + UUID.randomUUID();
        OxmqQueue<String> queue = OxmqQueue.<String>builder()
                .name(queueName)
                .connectionManager(connectionManager)
                .build();
        queuesToClean.add(queue);

        CountDownLatch jobStarted = new CountDownLatch(1);
        CountDownLatch jobInterrupted = new CountDownLatch(1);
        AtomicReference<String> cancelReasonRef = new AtomicReference<>();

        OxmqWorker<String> worker = OxmqWorker.<String>builder()
                .queueName(queueName)
                .connectionManager(connectionManager)
                .concurrency(2)
                .processor(job -> {
                    jobStarted.countDown();
                    try {
                        Thread.sleep(10_000);
                        return "ok";
                    } catch (InterruptedException e) {
                        jobInterrupted.countDown();
                        cancelReasonRef.set(job.getCancelReason());
                        throw e;
                    }
                })
                .build();
        workersToStop.add(worker);
        worker.start();

        JobOptions opts = JobOptions.builder().attempts(5).build();
        String jobId = queue.add("compute-task", "heavy-payload", opts).getId();

        assertThat(jobStarted.await(5, TimeUnit.SECONDS)).isTrue();
        assertThat(worker.isJobActive(jobId)).isTrue();
        assertThat(worker.getActiveJobCount()).isEqualTo(1);

        boolean cancelled = worker.cancelJob(jobId, "User requested abort");
        assertThat(cancelled).isTrue();

        assertThat(jobInterrupted.await(5, TimeUnit.SECONDS)).isTrue();
        assertThat(cancelReasonRef.get()).isEqualTo("User requested abort");

        // Wait for worker execution to finish and lock to clear
        TimeUnit.MILLISECONDS.sleep(300);

        assertThat(worker.isJobActive(jobId)).isFalse();
        assertThat(worker.getActiveJobCount()).isEqualTo(0);

        Job<?> job = queue.getJob(jobId);
        assertThat(job).isNotNull();
        assertThat(job.getState()).isEqualTo(JobState.FAILED);
        assertThat(job.getFailedReason()).contains("User requested abort");
    }

    @Test
    @DisplayName("Cooperative CancellationToken triggers callbacks and allows graceful termination with JobCancelledException")
    void testCooperativeCancellationToken() throws Exception {
        String queueName = "token-test-" + UUID.randomUUID();
        OxmqQueue<String> queue = OxmqQueue.<String>builder()
                .name(queueName)
                .connectionManager(connectionManager)
                .build();
        queuesToClean.add(queue);

        CountDownLatch jobStarted = new CountDownLatch(1);
        CountDownLatch callbackFired = new CountDownLatch(1);
        AtomicReference<String> callbackReason = new AtomicReference<>();

        OxmqWorker<String> worker = OxmqWorker.<String>builder()
                .queueName(queueName)
                .connectionManager(connectionManager)
                .processor(job -> {
                    job.getCancellationToken().onCancel(reason -> {
                        callbackReason.set(reason);
                        callbackFired.countDown();
                    });
                    jobStarted.countDown();

                    while (!job.isCancelled()) {
                        try {
                            Thread.sleep(50);
                        } catch (InterruptedException e) {
                            break;
                        }
                    }
                    throw new JobCancelledException(job.getCancelReason());
                })
                .build();
        workersToStop.add(worker);
        worker.start();

        String jobId = queue.add("loop-task", "data").getId();
        assertThat(jobStarted.await(5, TimeUnit.SECONDS)).isTrue();

        worker.cancelJob(jobId, "SLA budget exceeded");

        assertThat(callbackFired.await(5, TimeUnit.SECONDS)).isTrue();
        assertThat(callbackReason.get()).isEqualTo("SLA budget exceeded");

        TimeUnit.MILLISECONDS.sleep(300);

        Job<?> job = queue.getJob(jobId);
        assertThat(job).isNotNull();
        assertThat(job.getState()).isEqualTo(JobState.FAILED);
        assertThat(job.getFailedReason()).contains("SLA budget exceeded");
    }

    @Test
    @DisplayName("cancelAllJobs interrupts all active Virtual Threads concurrently")
    void testCancelAllJobs() throws Exception {
        String queueName = "cancel-all-test-" + UUID.randomUUID();
        OxmqQueue<String> queue = OxmqQueue.<String>builder()
                .name(queueName)
                .connectionManager(connectionManager)
                .build();
        queuesToClean.add(queue);

        int totalJobs = 4;
        CountDownLatch allStarted = new CountDownLatch(totalJobs);
        CountDownLatch allInterrupted = new CountDownLatch(totalJobs);

        OxmqWorker<String> worker = OxmqWorker.<String>builder()
                .queueName(queueName)
                .connectionManager(connectionManager)
                .concurrency(totalJobs)
                .useVirtualThreads(true)
                .processor(job -> {
                    allStarted.countDown();
                    try {
                        Thread.sleep(10_000);
                        return "done";
                    } catch (InterruptedException e) {
                        allInterrupted.countDown();
                        throw e;
                    }
                })
                .build();
        workersToStop.add(worker);
        worker.start();

        List<String> jobIds = new ArrayList<>();
        for (int i = 0; i < totalJobs; i++) {
            jobIds.add(queue.add("task-" + i, "data-" + i).getId());
        }

        assertThat(allStarted.await(5, TimeUnit.SECONDS)).isTrue();
        assertThat(worker.getActiveJobCount()).isEqualTo(totalJobs);

        int cancelledCount = worker.cancelAllJobs("Emergency graceful drain");
        assertThat(cancelledCount).isEqualTo(totalJobs);

        assertThat(allInterrupted.await(5, TimeUnit.SECONDS)).isTrue();
        TimeUnit.MILLISECONDS.sleep(400);

        assertThat(worker.getActiveJobCount()).isEqualTo(0);
        for (String id : jobIds) {
            Job<?> job = queue.getJob(id);
            assertThat(job.getState()).isEqualTo(JobState.FAILED);
            assertThat(job.getFailedReason()).contains("Emergency graceful drain");
        }
    }

    @Test
    @DisplayName("OxmqBatchWorker cancelJob interrupts active batch processing")
    void testBatchWorkerCancellation() throws Exception {
        String queueName = "batch-cancel-test-" + UUID.randomUUID();
        OxmqQueue<String> queue = OxmqQueue.<String>builder()
                .name(queueName)
                .connectionManager(connectionManager)
                .build();
        queuesToClean.add(queue);

        CountDownLatch batchStarted = new CountDownLatch(1);
        CountDownLatch batchInterrupted = new CountDownLatch(1);

        OxmqBatchWorker<String> batchWorker = OxmqBatchWorker.<String>builder()
                .queueName(queueName)
                .connectionManager(connectionManager)
                .batchSize(5)
                .processor(jobs -> {
                    batchStarted.countDown();
                    try {
                        Thread.sleep(10_000);
                        return "ok";
                    } catch (InterruptedException e) {
                        batchInterrupted.countDown();
                        throw e;
                    }
                })
                .build();
        workersToStop.add(batchWorker);
        batchWorker.start();

        List<String> ids = new ArrayList<>();
        for (int i = 0; i < 3; i++) {
            ids.add(queue.add("item-" + i, "data-" + i).getId());
        }

        assertThat(batchStarted.await(5, TimeUnit.SECONDS)).isTrue();
        assertThat(batchWorker.isJobActive(ids.get(0))).isTrue();
        assertThat(batchWorker.getActiveJobCount()).isEqualTo(3);

        boolean cancelled = batchWorker.cancelJob(ids.get(0), "Batch cancelled by operator");
        assertThat(cancelled).isTrue();

        assertThat(batchInterrupted.await(5, TimeUnit.SECONDS)).isTrue();
        TimeUnit.MILLISECONDS.sleep(400);

        assertThat(batchWorker.getActiveJobCount()).isEqualTo(0);
        for (String id : ids) {
            Job<?> job = queue.getJob(id);
            assertThat(job.getState()).isEqualTo(JobState.FAILED);
            assertThat(job.getFailedReason()).contains("Batch cancelled by operator");
        }
    }

    @Test
    @DisplayName("Cancelling non-existent or completed job safely returns false")
    void testCancelNonExistentJob() {
        String queueName = "cancel-noop-" + UUID.randomUUID();
        OxmqWorker<String> worker = OxmqWorker.<String>builder()
                .queueName(queueName)
                .connectionManager(connectionManager)
                .processor(job -> "ok")
                .build();
        workersToStop.add(worker);

        assertThat(worker.cancelJob("non-existent-id")).isFalse();
        assertThat(worker.cancelJob(null)).isFalse();
        assertThat(worker.isJobActive("non-existent-id")).isFalse();
        assertThat(worker.getActiveJobCount()).isEqualTo(0);
    }
}
