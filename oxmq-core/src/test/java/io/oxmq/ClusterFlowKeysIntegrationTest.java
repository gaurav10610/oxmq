package io.oxmq;

import io.oxmq.client.QueueKeys;
import io.oxmq.client.RedisConnectionManager;
import io.oxmq.model.FlowJob;
import io.oxmq.model.Job;
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

import static org.assertj.core.api.Assertions.assertThat;

public class ClusterFlowKeysIntegrationTest {

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
    @DisplayName("QueueKeys.forClusterFlow generates shared hash tag across multiple distinct queues")
    void testForClusterFlowKeys() {
        String clusterTag = "order-pipeline";
        QueueKeys parentKeys = QueueKeys.forClusterFlow(clusterTag, "bull", "parent-queue");
        QueueKeys childKeys = QueueKeys.forClusterFlow(clusterTag, "bull", "child-queue");

        assertThat(parentKeys.getQualifiedName()).isEqualTo("{order-pipeline}:bull:parent-queue");
        assertThat(childKeys.getQualifiedName()).isEqualTo("{order-pipeline}:bull:child-queue");

        assertThat(parentKeys.isClusterTagged()).isTrue();
        assertThat(childKeys.isClusterTagged()).isTrue();

        // Both keys share the identical CRC16 hash tag "{order-pipeline}"
        assertThat(parentKeys.getKey("wait")).startsWith("{order-pipeline}:bull:parent-queue:wait");
        assertThat(childKeys.getKey("wait")).startsWith("{order-pipeline}:bull:child-queue:wait");
    }

    @Test
    @DisplayName("FlowProducer.forCluster creates multi-queue DAG sharing cluster hash tag and resolves dependencies")
    void testClusterFlowProducerDAGExecution() throws Exception {
        String flowTag = "checkout-" + UUID.randomUUID().toString().substring(0, 8);
        String parentQueueName = "orders-" + UUID.randomUUID();
        String childQueueName = "inventory-" + UUID.randomUUID();

        String parentPrefix = "{" + flowTag + "}:bull";
        String childPrefix = "{" + flowTag + "}:bull";

        OxmqQueue<String> parentQueue = OxmqQueue.<String>builder()
                .prefix(parentPrefix)
                .name(parentQueueName)
                .connectionManager(connectionManager)
                .build();
        OxmqQueue<String> childQueue = OxmqQueue.<String>builder()
                .prefix(childPrefix)
                .name(childQueueName)
                .connectionManager(connectionManager)
                .build();
        queuesToClean.add(parentQueue);
        queuesToClean.add(childQueue);

        FlowProducer flowProducer = FlowProducer.forCluster(flowTag, connectionManager);
        assertThat(flowProducer.getClusterTag()).isEqualTo(flowTag);

        // Build DAG: Order (parent) depends on Inventory (child)
        FlowJob<String> child = FlowJob.of(childQueueName, "reserve-stock", "item-sku-100");
        FlowJob<String> parent = FlowJob.of(parentQueueName, "charge-order", "order-101");
        parent.addChild(child);

        String parentId = flowProducer.add(parent);
        assertThat(parentId).isNotNull();

        Job<?> parentJob = parentQueue.getJob(parentId);
        assertThat(parentJob).isNotNull();
        assertThat(parentJob.getState()).isEqualTo(JobState.WAITING_CHILDREN);

        // Consume and complete the child job on inventory queue
        CountDownLatch childDone = new CountDownLatch(1);
        OxmqWorker<String> childWorker = OxmqWorker.<String>builder()
                .prefix(childPrefix)
                .queueName(childQueueName)
                .connectionManager(connectionManager)
                .processor(job -> {
                    childDone.countDown();
                    return "stock-reserved";
                })
                .build();
        workersToStop.add(childWorker);
        childWorker.start();

        assertThat(childDone.await(5, TimeUnit.SECONDS)).isTrue();
        TimeUnit.MILLISECONDS.sleep(300);

        // Child completion should have promoted parent from WAITING_CHILDREN to WAITING
        Job<?> updatedParentJob = parentQueue.getJob(parentId);
        assertThat(updatedParentJob.getState()).isEqualTo(JobState.WAITING);
    }
}
