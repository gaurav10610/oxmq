package io.oxmq.spring.actuator;

import io.oxmq.spring.OxmqTemplate;
import org.springframework.boot.actuate.endpoint.annotation.Endpoint;
import org.springframework.boot.actuate.endpoint.annotation.ReadOperation;
import org.springframework.boot.actuate.endpoint.annotation.Selector;
import org.springframework.boot.actuate.endpoint.annotation.WriteOperation;
import org.springframework.lang.Nullable;

import java.time.Duration;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Spring Boot Actuator endpoint exposing virtual queue groups management over REST / JMX:
 * {@code /actuator/oxmq-groups}
 */
@Endpoint(id = "oxmq-groups")
public class OxmqGroupsEndpoint {

    private final OxmqTemplate oxmqTemplate;

    public OxmqGroupsEndpoint(OxmqTemplate oxmqTemplate) {
        this.oxmqTemplate = oxmqTemplate;
    }

    /**
     * Lists virtual groups and breakdown of pending job counts across groups for a queue.
     * GET /actuator/oxmq-groups?queue=notifications&limit=50
     */
    @ReadOperation
    public Map<String, Object> getGroupsOverview(String queue, @Nullable Integer limit) {
        if (queue == null || queue.isBlank()) {
            return Map.of("error", "Query parameter 'queue' is required");
        }
        int max = (limit != null && limit > 0) ? limit : 100;
        List<String> groups = oxmqTemplate.getGroups(queue, 0, max - 1);
        Map<String, Long> jobCounts = oxmqTemplate.getGroupsJobsCount(queue, max);

        Map<String, Object> result = new LinkedHashMap<>();
        result.put("queue", queue);
        result.put("totalGroupsReturned", groups.size());
        result.put("groups", groups);
        result.put("jobsCountPerGroup", jobCounts);
        return result;
    }

    /**
     * Inspects details of a specific virtual group.
     * GET /actuator/oxmq-groups/{groupId}?queue=notifications
     */
    @ReadOperation
    public Map<String, Object> getGroupDetails(@Selector String groupId, String queue) {
        if (queue == null || queue.isBlank()) {
            return Map.of("error", "Query parameter 'queue' is required");
        }
        long jobCount = oxmqTemplate.getGroupJobsCount(queue, groupId);
        long rateLimitTtl = oxmqTemplate.getGroupRateLimitTtl(queue, groupId);
        Integer concurrencyOverride = oxmqTemplate.getGroupConcurrency(queue, groupId);

        Map<String, Object> details = new LinkedHashMap<>();
        details.put("queue", queue);
        details.put("groupId", groupId);
        details.put("waitingJobsCount", jobCount);
        details.put("rateLimitTtlMs", rateLimitTtl);
        details.put("isRateLimited", rateLimitTtl > 0);
        details.put("concurrencyOverride", concurrencyOverride);
        return details;
    }

    /**
     * Performs management operations on virtual groups:
     * <ul>
     *   <li>action="pause": pauses the group</li>
     *   <li>action="resume": resumes the group</li>
     *   <li>action="repair": repairs stuck maxed group (or all if groupId omitted)</li>
     *   <li>action="setRateLimit", durationMs=5000: applies rate limit</li>
     *   <li>action="setConcurrency", concurrency=5: dynamic concurrency override</li>
     *   <li>action="removeConcurrency": removes concurrency override</li>
     * </ul>
     * POST /actuator/oxmq-groups
     */
    public Map<String, Object> manageGroup(String queue,
                                          String action,
                                          @Nullable String groupId,
                                          @Nullable Long durationMs,
                                          @Nullable Integer concurrency) {
        return manageGroup(queue, action, groupId, durationMs, concurrency, null);
    }

    @WriteOperation
    public Map<String, Object> manageGroup(String queue,
                                          String action,
                                          @Nullable String groupId,
                                          @Nullable Long durationMs,
                                          @Nullable Integer concurrency,
                                          @Nullable Integer maxJobs) {
        if (queue == null || queue.isBlank()) {
            return Map.of("error", "Property 'queue' is required");
        }
        if (action == null || action.isBlank()) {
            return Map.of("error", "Property 'action' is required (pause, resume, repair, setRateLimit, setConcurrency, removeConcurrency)");
        }

        Map<String, Object> response = new LinkedHashMap<>();
        response.put("queue", queue);
        response.put("action", action);
        response.put("groupId", groupId);

        switch (action.toLowerCase()) {
            case "pause" -> {
                if (groupId == null || groupId.isBlank()) {
                    return Map.of("error", "Property 'groupId' is required for action 'pause'");
                }
                boolean paused = oxmqTemplate.pauseGroup(queue, groupId);
                response.put("success", paused);
            }
            case "resume" -> {
                if (groupId == null || groupId.isBlank()) {
                    return Map.of("error", "Property 'groupId' is required for action 'resume'");
                }
                boolean resumed = oxmqTemplate.resumeGroup(queue, groupId);
                response.put("success", resumed);
            }
            case "repair" -> {
                if (groupId != null && !groupId.isBlank()) {
                    boolean repaired = oxmqTemplate.repairGroup(queue, groupId);
                    response.put("success", repaired);
                    response.put("repairedCount", repaired ? 1 : 0);
                } else {
                    int repairedCount = oxmqTemplate.repairMaxedGroups(queue);
                    response.put("success", true);
                    response.put("repairedCount", repairedCount);
                }
            }
            case "setratelimit" -> {
                if (groupId == null || groupId.isBlank()) {
                    return Map.of("error", "Property 'groupId' is required for action 'setRateLimit'");
                }
                long ms = (durationMs != null && durationMs > 0) ? durationMs : 1000;
                int max = (maxJobs != null && maxJobs > 0) ? maxJobs : 1;
                boolean limited = oxmqTemplate.setGroupRateLimit(queue, groupId, max, Duration.ofMillis(ms));
                response.put("success", limited);
                response.put("durationMs", ms);
                response.put("maxJobs", max);
            }
            case "setconcurrency" -> {
                if (groupId == null || groupId.isBlank() || concurrency == null) {
                    return Map.of("error", "Properties 'groupId' and 'concurrency' are required for action 'setConcurrency'");
                }
                oxmqTemplate.setGroupConcurrency(queue, groupId, concurrency);
                response.put("success", true);
                response.put("concurrency", concurrency);
            }
            case "removeconcurrency" -> {
                if (groupId == null || groupId.isBlank()) {
                    return Map.of("error", "Property 'groupId' is required for action 'removeConcurrency'");
                }
                boolean removed = oxmqTemplate.removeGroupConcurrency(queue, groupId);
                response.put("success", removed);
            }
            default -> {
                response.put("error", "Unknown action: " + action);
                response.put("success", false);
            }
        }
        return response;
    }
}
