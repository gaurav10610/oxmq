package io.oxmq;

import io.oxmq.client.RedisConnectionManager;
import io.oxmq.model.FlowJob;
import io.oxmq.model.Job;
import io.oxmq.model.JobState;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.junit.jupiter.api.Assertions.*;

@DisplayName("Dynamic Runtime Child Spawning in DAGs Integration Tests")
class DynamicFlowChildSpawningIntegrationTest {

    private String queueName;
    private RedisConnectionManager connectionManager;
    private OxmqQueue<String> queue;
    private OxmqWorker<String> worker;

    @BeforeEach
    void setUp() {
        queueName = "dynamic-flow-queue-" + UUID.randomUUID();
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
    @DisplayName("Active job dynamically spawns children, yields to waiting-children, and completes when children finish")
    void testDynamicChildSpawningAndParentCompletion() throws Exception {
        AtomicBoolean childSpawned = new AtomicBoolean(false);
        List<String> executionLog = Collections.synchronizedList(new ArrayList<>());
        CountDownLatch parentFinishedLatch = new CountDownLatch(1);

        worker = OxmqWorker.<String>builder()
                .queueName(queueName)
                .connectionManager(connectionManager)
                .concurrency(2)
                .processor(job -> {
                    if ("parent-job".equals(job.getName())) {
                        if (!childSpawned.getAndSet(true)) {
                            executionLog.add("parent-spawning-child");
                            FlowJob<String> child = FlowJob.of(queueName, "child-task-1", "child-payload");
                            job.addChild(child);
                            return "unreachable";
                        } else {
                            executionLog.add("parent-resumed-and-finished");
                            parentFinishedLatch.countDown();
                            return "parent-complete";
                        }
                    } else if ("child-task-1".equals(job.getName())) {
                        executionLog.add("child-executed");
                        return "child-complete";
                    }
                    return "ok";
                })
                .build();

        worker.start();

        Job<String> parent = queue.add("parent-job", "parent-payload");
        assertTrue(parentFinishedLatch.await(10, TimeUnit.SECONDS), "Parent job should finish after dynamically spawned child completes");

        assertEquals(List.of("parent-spawning-child", "child-executed", "parent-resumed-and-finished"), executionLog,
                "Dynamic flow execution order: parent spawns child -> child completes -> parent resumes and completes");

        Job<String> completedParent = queue.getJob(parent.getId());
        assertEquals(JobState.COMPLETED, completedParent.getState());
    }
}
