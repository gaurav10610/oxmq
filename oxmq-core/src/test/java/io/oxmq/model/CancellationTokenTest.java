package io.oxmq.model;

import io.oxmq.exception.JobCancelledException;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.*;

class CancellationTokenTest {

    @Test
    @DisplayName("CancellationToken: cancel triggers callbacks once and throwIfCancelled throws")
    void testCancelBasic() {
        CancellationToken token = new CancellationToken();
        assertFalse(token.isCancelled());

        AtomicInteger callCount = new AtomicInteger(0);
        token.onCancel(reason -> callCount.incrementAndGet());

        token.cancel("User aborted");
        assertTrue(token.isCancelled());
        assertEquals("User aborted", token.getReason());
        assertEquals(1, callCount.get());

        // Second cancel should be a no-op
        token.cancel("Second abort");
        assertEquals(1, callCount.get());

        // onCancel after already cancelled should invoke immediately once
        token.onCancel(reason -> callCount.incrementAndGet());
        assertEquals(2, callCount.get());

        assertThrows(JobCancelledException.class, token::throwIfCancelled);
    }

    @Test
    @DisplayName("CancellationToken: concurrent cancel and onCancel never causes duplicate invocations")
    void testConcurrentCancelAndOnCancel() throws InterruptedException {
        int threads = 10;
        int iterations = 1000;
        ExecutorService executor = Executors.newFixedThreadPool(threads);

        try {
            for (int i = 0; i < iterations; i++) {
                CancellationToken token = new CancellationToken();
                AtomicInteger callbackCounter = new AtomicInteger(0);
                CountDownLatch readyLatch = new CountDownLatch(threads);
                CountDownLatch startLatch = new CountDownLatch(1);

                for (int t = 0; t < threads - 1; t++) {
                    executor.submit(() -> {
                        readyLatch.countDown();
                        try {
                            startLatch.await();
                            token.onCancel(r -> callbackCounter.incrementAndGet());
                        } catch (InterruptedException e) {
                            Thread.currentThread().interrupt();
                        }
                    });
                }

                // Dedicated thread calling cancel
                executor.submit(() -> {
                    readyLatch.countDown();
                    try {
                        startLatch.await();
                        token.cancel("Concurrent Cancel");
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                    }
                });

                assertTrue(readyLatch.await(5, TimeUnit.SECONDS));
                startLatch.countDown();

                // Wait briefly for all threads
                TimeUnit.MILLISECONDS.sleep(5);
                token.cancel("Final Cancel");

                // Each of the (threads - 1) callbacks must have run EXACTLY once
                assertEquals(threads - 1, callbackCounter.get(),
                        "Iteration " + i + ": expected exactly " + (threads - 1) + " callbacks, got " + callbackCounter.get());
            }
        } finally {
            executor.shutdownNow();
        }
    }
}
