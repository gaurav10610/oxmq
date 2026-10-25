package io.oxmq.spring.actuator;

import io.lettuce.core.RedisClient;
import io.oxmq.client.RedisConnectionManager;
import io.oxmq.lua.LuaScriptManager;
import io.oxmq.metrics.OxmqMetrics;
import io.oxmq.model.JobOptions;
import io.oxmq.serializer.JacksonJobSerializer;
import io.oxmq.spring.OxmqTemplate;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

@DisplayName("OxmqGroupsEndpoint Actuator REST Operations Integration Test")
class OxmqGroupsEndpointIntegrationTest {

    private String queueName;
    private RedisClient redisClient;
    private RedisConnectionManager connectionManager;
    private OxmqTemplate oxmqTemplate;
    private OxmqGroupsEndpoint endpoint;

    @BeforeEach
    void setUp() {
        queueName = "actuator-groups-" + UUID.randomUUID();
        redisClient = RedisClient.create("redis://localhost:6379");
        connectionManager = new RedisConnectionManager(redisClient);
        oxmqTemplate = new OxmqTemplate(
                redisClient,
                connectionManager,
                new LuaScriptManager(),
                new JacksonJobSerializer(),
                new OxmqMetrics(null),
                null,
                "bull",
                null,
                -1
        );
        endpoint = new OxmqGroupsEndpoint(oxmqTemplate);
    }

    @AfterEach
    void tearDown() {
        if (oxmqTemplate != null) {
            try { oxmqTemplate.obliterate(queueName); } catch (Exception ignored) {}
        }
        if (connectionManager != null) {
            try { connectionManager.close(); } catch (Exception ignored) {}
        }
        if (redisClient != null) {
            try { redisClient.shutdown(); } catch (Exception ignored) {}
        }
    }

    @Test
    @DisplayName("Endpoint provides groups overview and jobs count breakdown")
    void testGroupsOverviewAndDetails() {
        oxmqTemplate.add(queueName, "job-1", "d1", JobOptions.builder().group("tenant-acme").build());
        oxmqTemplate.add(queueName, "job-2", "d2", JobOptions.builder().group("tenant-acme").build());
        oxmqTemplate.add(queueName, "job-3", "d3", JobOptions.builder().group("tenant-globex").build());

        Map<String, Object> overview = endpoint.getGroupsOverview(queueName, 50);
        assertThat(overview.get("queue")).isEqualTo(queueName);
        assertThat(overview.get("totalGroupsReturned")).isEqualTo(2);

        @SuppressWarnings("unchecked")
        List<String> groups = (List<String>) overview.get("groups");
        assertThat(groups).contains("tenant-acme", "tenant-globex");

        Map<?, ?> counts = (Map<?, ?>) overview.get("jobsCountPerGroup");
        assertThat(counts.get("tenant-acme")).isEqualTo(2L);
        assertThat(counts.get("tenant-globex")).isEqualTo(1L);

        // Test details endpoint
        Map<String, Object> details = endpoint.getGroupDetails("tenant-acme", queueName);
        assertThat(details.get("groupId")).isEqualTo("tenant-acme");
        assertThat(details.get("waitingJobsCount")).isEqualTo(2L);
        assertThat(details.get("isRateLimited")).isEqualTo(false);
    }

    @Test
    @DisplayName("Endpoint performs group management operations: rate limit, concurrency, pause, resume, repair")
    void testGroupManagementOperations() {
        String group = "tenant-ops";
        oxmqTemplate.add(queueName, "job-1", "payload", JobOptions.builder().group(group).build());

        // 1. Set Rate Limit
        Map<String, Object> rateLimitRes = endpoint.manageGroup(queueName, "setRateLimit", group, 5000L, null);
        assertThat(rateLimitRes.get("success")).isEqualTo(true);

        Map<String, Object> details = endpoint.getGroupDetails(group, queueName);
        assertThat(details.get("isRateLimited")).isEqualTo(true);
        assertThat((Long) details.get("rateLimitTtlMs")).isGreaterThan(0L);

        // 2. Pause and Resume Group
        Map<String, Object> pauseRes = endpoint.manageGroup(queueName, "pause", group, null, null);
        assertThat(pauseRes.get("success")).isEqualTo(true);

        Map<String, Object> resumeRes = endpoint.manageGroup(queueName, "resume", group, null, null);
        assertThat(resumeRes.get("success")).isEqualTo(true);

        // 3. Set and Remove Concurrency Override
        Map<String, Object> setConcRes = endpoint.manageGroup(queueName, "setConcurrency", group, null, 7);
        assertThat(setConcRes.get("success")).isEqualTo(true);
        assertThat(oxmqTemplate.getGroupConcurrency(queueName, group)).isEqualTo(7);

        Map<String, Object> remConcRes = endpoint.manageGroup(queueName, "removeConcurrency", group, null, null);
        assertThat(remConcRes.get("success")).isEqualTo(true);
        assertThat(oxmqTemplate.getGroupConcurrency(queueName, group)).isNull();

        // 4. Group Self-Healing Repair
        var sync = connectionManager.getCommandConnection().sync();
        sync.sadd("bull:" + queueName + ":groups:maxed", "orphaned-tenant");
        sync.hset("bull:" + queueName + ":groups:active:count", "orphaned-tenant", "1");

        Map<String, Object> repairRes = endpoint.manageGroup(queueName, "repair", "orphaned-tenant", null, null);
        assertThat(repairRes.get("success")).isEqualTo(true);
        assertThat(repairRes.get("repairedCount")).isEqualTo(1);
    }
}
