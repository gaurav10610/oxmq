package io.oxmq.interceptor;

import io.oxmq.OxmqQueue;
import io.oxmq.OxmqWorker;
import io.oxmq.client.RedisConnectionManager;
import io.oxmq.model.Job;
import io.oxmq.model.JobOptions;
import java.time.Duration;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

public class JobInterceptorTest {

    private RedisConnectionManager connectionManager;
    private String queueName;
    private OxmqQueue<String> queue;
    private OxmqWorker<String> worker;

    @BeforeEach
    void setUp() {
        connectionManager = new RedisConnectionManager("redis://localhost:6379");
        queueName = "test-interceptor-" + System.currentTimeMillis();
        queue = OxmqQueue.<String>builder()
                .name(queueName)
                .connectionManager(connectionManager)
                .build();
    }

    @AfterEach
    void tearDown() {
        if (worker != null) {
            worker.close();
        }
        if (queue != null) {
            queue.obliterate();
            queue.close();
        }
        if (connectionManager != null) {
            connectionManager.close();
        }
    }

    @Test
    void testInterceptorLifecycle_ProceedAndAfterProcess() throws Exception {
        AtomicBoolean beforeCalled = new AtomicBoolean(false);
        AtomicBoolean afterCalled = new AtomicBoolean(false);
        CountDownLatch latch = new CountDownLatch(1);

        JobInterceptor testInterceptor = new JobInterceptor() {
            @Override
            public InterceptorDecision beforeProcess(Job<?> job) {
                beforeCalled.set(true);
                return InterceptorDecision.proceed();
            }

            @Override
            public void afterProcess(Job<?> job, Object result, Duration duration) {
                afterCalled.set(true);
                latch.countDown();
            }
        };

        worker = OxmqWorker.<String>builder()
                .queueName(queueName)
                .connectionManager(connectionManager)
                .interceptor(testInterceptor)
                .processor(job -> "processed-" + job.getData())
                .concurrency(1)
                .build();

        worker.start();
        queue.add("job-1", "test-payload");

        assertTrue(latch.await(5, TimeUnit.SECONDS), "Job should complete and trigger afterProcess");
        assertTrue(beforeCalled.get(), "beforeProcess should have been called");
        assertTrue(afterCalled.get(), "afterProcess should have been called");
    }

    @Test
    void testInterceptor_DelayPreemption() throws Exception {
        AtomicInteger beforeCount = new AtomicInteger(0);
        CountDownLatch latch = new CountDownLatch(1);

        JobInterceptor delayOnceInterceptor = new JobInterceptor() {
            @Override
            public InterceptorDecision beforeProcess(Job<?> job) {
                int count = beforeCount.incrementAndGet();
                if (count == 1) {
                    // First time: intercept and delay by 200 ms with skipAttempt=true
                    return InterceptorDecision.delay(200, true);
                }
                // Second time (after delay): allow processing
                return InterceptorDecision.proceed();
            }

            @Override
            public void afterProcess(Job<?> job, Object result, Duration duration) {
                latch.countDown();
            }
        };

        worker = OxmqWorker.<String>builder()
                .queueName(queueName)
                .connectionManager(connectionManager)
                .interceptor(delayOnceInterceptor)
                .processor(job -> "ok")
                .pollIntervalMs(20)
                .concurrency(1)
                .build();

        worker.start();
        queue.add("job-delayed", "payload");

        assertTrue(latch.await(5, TimeUnit.SECONDS), "Job should eventually complete after being delayed");
        assertEquals(2, beforeCount.get(), "Job should have hit beforeProcess twice (once delayed, once executed)");
    }
}
