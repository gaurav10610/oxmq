package io.oxmq.spring;

import io.oxmq.OxmqQueue;
import io.oxmq.model.Job;
import io.oxmq.spring.annotation.EnableOxmq;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.test.context.SpringBootTest;

import static org.assertj.core.api.Assertions.assertThat;

@SpringBootTest(classes = MemoryOptimizationSpringBootTest.TestApp.class, properties = {
        "spring.application.name=memory-opt-service",
        "oxmq.redis.uri=redis://localhost:6379",
        "oxmq.max-len-events=2500",
        "oxmq.default-job-options.remove-on-complete-count=750",
        "oxmq.default-job-options.remove-on-complete-age-seconds=86400",
        "oxmq.default-job-options.remove-on-fail-count=3000"
})
public class MemoryOptimizationSpringBootTest {

    @SpringBootApplication
    @EnableOxmq
    static class TestApp {}

    @Autowired
    private OxmqTemplate oxmqTemplate;

    @Autowired
    private OxmqProperties oxmqProperties;

    @AfterEach
    void tearDown() {
        try {
            oxmqTemplate.getQueue("mem-test-queue").obliterate();
        } catch (Exception ignored) {}
    }

    @Test
    @DisplayName("Verify Spring Boot binds maxLenEvents and defaultJobOptions properties")
    void testPropertiesBinding() {
        assertThat(oxmqProperties.getMaxLenEvents()).isEqualTo(2500);
        assertThat(oxmqProperties.getDefaultJobOptions()).isNotNull();
        assertThat(oxmqProperties.getDefaultJobOptions().getRemoveOnCompleteCount()).isEqualTo(750);
        assertThat(oxmqProperties.getDefaultJobOptions().getRemoveOnCompleteAgeSeconds()).isEqualTo(86400L);
        assertThat(oxmqProperties.getDefaultJobOptions().getRemoveOnFailCount()).isEqualTo(3000);
    }

    @Test
    @DisplayName("Verify OxmqTemplate creates queues inheriting default retention options")
    void testTemplateQueueRetentionInheritance() {
        OxmqQueue<String> queue = oxmqTemplate.getQueue("mem-test-queue", String.class);
        assertThat(queue).isNotNull();
        assertThat(queue.getDefaultJobOptions()).isNotNull();

        Job<String> job = queue.add("auto-retained-job", "sample-data");
        assertThat(job).isNotNull();
        assertThat(job.getOpts()).isNotNull();
        assertThat(job.getOpts().getKeepCompletedJobs()).isNotNull();
        assertThat(job.getOpts().getKeepCompletedJobs().getCount()).isEqualTo(750);
        assertThat(job.getOpts().getKeepCompletedJobs().getAgeSeconds()).isEqualTo(86400L);
        assertThat(job.getOpts().getKeepFailedJobs()).isNotNull();
        assertThat(job.getOpts().getKeepFailedJobs().getCount()).isEqualTo(3000);
    }
}
