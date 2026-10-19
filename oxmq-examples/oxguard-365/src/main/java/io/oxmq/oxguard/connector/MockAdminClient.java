package io.oxmq.oxguard.connector;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.Base64;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

/**
 * REST Client for administering the decoupled Mock Office 365 Server.
 * Connects to http://localhost:8081/mock-admin to dynamically adjust rate limits,
 * trigger 429 burst scenarios, scale seeded files, and simulate forward uploads without
 * coupling the DSPM production code to mock server internals.
 */
@Component
public class MockAdminClient {

    private static final Logger log = LoggerFactory.getLogger(MockAdminClient.class);

    private final String mockAdminBaseUrl;
    private final HttpClient httpClient;
    private final ObjectMapper objectMapper;

    public MockAdminClient(
            @Value("${oxguard.mock-admin.base-url:http://localhost:8081/mock-admin}") String mockAdminBaseUrl,
            ObjectMapper objectMapper
    ) {
        this.mockAdminBaseUrl = mockAdminBaseUrl.endsWith("/") ? mockAdminBaseUrl.substring(0, mockAdminBaseUrl.length() - 1) : mockAdminBaseUrl;
        this.httpClient = HttpClient.newBuilder()
                .connectTimeout(Duration.ofSeconds(3))
                .build();
        this.objectMapper = objectMapper;
    }

    public boolean setRateLimit(String tenantId, int limit) {
        try {
            HttpRequest request = HttpRequest.newBuilder()
                    .uri(URI.create(mockAdminBaseUrl + "/set-rate-limit?tenantId=" + tenantId + "&limit=" + limit))
                    .POST(HttpRequest.BodyPublishers.noBody())
                    .timeout(Duration.ofSeconds(5))
                    .build();
            HttpResponse<String> resp = httpClient.send(request, HttpResponse.BodyHandlers.ofString());
            return resp.statusCode() == 200;
        } catch (Exception e) {
            log.warn("Failed setting mock rate limit for [{}]: {}", tenantId, e.getMessage());
            return false;
        }
    }

    public boolean setGlobalRateLimit(int limit) {
        return setRateLimit("all", limit);
    }

    public boolean trigger429Burst(String tenantId, int count, boolean enabled) {
        try {
            HttpRequest request = HttpRequest.newBuilder()
                    .uri(URI.create(mockAdminBaseUrl + "/simulate-429?tenantId=" + tenantId + "&burstCount=" + count + "&enabled=" + enabled))
                    .POST(HttpRequest.BodyPublishers.noBody())
                    .timeout(Duration.ofSeconds(5))
                    .build();
            HttpResponse<String> resp = httpClient.send(request, HttpResponse.BodyHandlers.ofString());
            return resp.statusCode() == 200;
        } catch (Exception e) {
            log.warn("Failed triggering mock 429 burst for [{}]: {}", tenantId, e.getMessage());
            return false;
        }
    }

    public boolean injectChaos(String tenantId, int statusCode, int count, int retryAfter) {
        try {
            HttpRequest request = HttpRequest.newBuilder()
                    .uri(URI.create(mockAdminBaseUrl + "/chaos?tenantId=" + tenantId + "&status=" + statusCode + "&count=" + count + "&retryAfter=" + retryAfter))
                    .POST(HttpRequest.BodyPublishers.noBody())
                    .timeout(Duration.ofSeconds(5))
                    .build();
            HttpResponse<String> resp = httpClient.send(request, HttpResponse.BodyHandlers.ofString());
            return resp.statusCode() == 200;
        } catch (Exception e) {
            log.warn("Failed injecting chaos (status: {}) for [{}]: {}", statusCode, tenantId, e.getMessage());
            return false;
        }
    }

    public boolean setRandomChaos(String tenantId, double randomRate) {
        try {
            HttpRequest request = HttpRequest.newBuilder()
                    .uri(URI.create(mockAdminBaseUrl + "/chaos?tenantId=" + tenantId + "&randomRate=" + randomRate))
                    .POST(HttpRequest.BodyPublishers.noBody())
                    .timeout(Duration.ofSeconds(5))
                    .build();
            HttpResponse<String> resp = httpClient.send(request, HttpResponse.BodyHandlers.ofString());
            return resp.statusCode() == 200;
        } catch (Exception e) {
            log.warn("Failed setting random chaos for [{}]: {}", tenantId, e.getMessage());
            return false;
        }
    }

    public boolean seedAdditionalFiles(int count) {
        try {
            HttpRequest request = HttpRequest.newBuilder()
                    .uri(URI.create(mockAdminBaseUrl + "/seed?count=" + count))
                    .POST(HttpRequest.BodyPublishers.noBody())
                    .timeout(Duration.ofSeconds(10))
                    .build();
            HttpResponse<String> resp = httpClient.send(request, HttpResponse.BodyHandlers.ofString());
            return resp.statusCode() == 200;
        } catch (Exception e) {
            log.warn("Failed seeding mock files: {}", e.getMessage());
            return false;
        }
    }

    public boolean uploadForwardFile(String driveId, String fileName, String exposure, byte[] fileBytes, String mimeType) {
        try {
            ObjectNode root = objectMapper.createObjectNode();
            root.put("driveId", driveId);
            root.put("fileName", fileName);
            root.put("exposure", exposure);
            root.put("mimeType", mimeType);
            root.put("contentBase64", Base64.getEncoder().encodeToString(fileBytes));

            HttpRequest request = HttpRequest.newBuilder()
                    .uri(URI.create(mockAdminBaseUrl + "/upload-forward-file"))
                    .header("Content-Type", "application/json")
                    .POST(HttpRequest.BodyPublishers.ofString(root.toString()))
                    .timeout(Duration.ofSeconds(5))
                    .build();
            HttpResponse<String> resp = httpClient.send(request, HttpResponse.BodyHandlers.ofString());
            return resp.statusCode() == 200;
        } catch (Exception e) {
            log.warn("Failed uploading mock forward file: {}", e.getMessage());
            return false;
        }
    }

    @SuppressWarnings("unchecked")
    public Map<String, Object> getStats() {
        try {
            HttpRequest request = HttpRequest.newBuilder()
                    .uri(URI.create(mockAdminBaseUrl + "/stats"))
                    .GET()
                    .timeout(Duration.ofSeconds(3))
                    .build();
            HttpResponse<String> resp = httpClient.send(request, HttpResponse.BodyHandlers.ofString());
            if (resp.statusCode() == 200) {
                return objectMapper.readValue(resp.body(), Map.class);
            }
        } catch (Exception e) {
            log.debug("Mock server not reachable at {}: {}", mockAdminBaseUrl, e.getMessage());
        }
        return Map.of("status", "UNAVAILABLE");
    }

    public boolean isAvailable() {
        Map<String, Object> stats = getStats();
        return "UP".equals(stats.get("status"));
    }
}
