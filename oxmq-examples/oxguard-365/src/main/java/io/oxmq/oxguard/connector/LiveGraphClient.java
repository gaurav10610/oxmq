package io.oxmq.oxguard.connector;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.oxmq.oxguard.model.DriveItem;
import io.oxmq.oxguard.model.DriveType;
import io.oxmq.oxguard.model.EffectiveExposure;
import io.oxmq.oxguard.model.ExposureLevel;
import io.oxmq.oxguard.model.MonitoredDrive;
import io.oxmq.oxguard.model.PermissionEntry;
import java.io.InputStream;
import java.net.URI;
import java.net.URLEncoder;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Component;

/**
 * Production Microsoft Graph API v1.0 Client.
 * Implements OAuth2 Client Credentials grant, SAS download URL bypass, and HTTP 429 Retry-After handling.
 */
@Component
@ConditionalOnProperty(name = "oxguard.client.mode", havingValue = "LIVE")
public class LiveGraphClient implements GraphClient {

    private static final Logger log = LoggerFactory.getLogger(LiveGraphClient.class);

    private final String tenantId;
    private final String clientId;
    private final String clientSecret;
    private final String graphBaseUrl;
    private final String loginBaseUrl;
    private final HttpClient httpClient;
    private final ObjectMapper objectMapper;

    private volatile String cachedAccessToken;
    private volatile long tokenExpiryEpochMs = 0;

    public LiveGraphClient(
            @Value("${oxguard.azure.tenant-id:}") String tenantId,
            @Value("${oxguard.azure.client-id:}") String clientId,
            @Value("${oxguard.azure.client-secret:}") String clientSecret,
            @Value("${oxguard.azure.graph-base-url:https://graph.microsoft.com/v1.0}") String graphBaseUrl,
            @Value("${oxguard.azure.login-base-url:https://login.microsoftonline.com}") String loginBaseUrl,
            ObjectMapper objectMapper
    ) {
        this.tenantId = tenantId;
        this.clientId = clientId;
        this.clientSecret = clientSecret;
        this.graphBaseUrl = graphBaseUrl;
        this.loginBaseUrl = loginBaseUrl;
        this.objectMapper = objectMapper;
        this.httpClient = HttpClient.newBuilder()
                .connectTimeout(Duration.ofSeconds(10))
                .followRedirects(HttpClient.Redirect.NORMAL)
                .build();
    }

    private synchronized String getValidAccessToken() {
        long now = System.currentTimeMillis();
        if (cachedAccessToken != null && now < (tokenExpiryEpochMs - 60_000)) {
            return cachedAccessToken;
        }

        try {
            String tokenEndpoint = loginBaseUrl + "/" + tenantId + "/oauth2/v2.0/token";
            String form = "client_id=" + URLEncoder.encode(clientId, StandardCharsets.UTF_8)
                    + "&client_secret=" + URLEncoder.encode(clientSecret, StandardCharsets.UTF_8)
                    + "&scope=" + URLEncoder.encode(graphBaseUrl + "/.default", StandardCharsets.UTF_8)
                    + "&grant_type=client_credentials";

            HttpRequest request = HttpRequest.newBuilder()
                    .uri(URI.create(tokenEndpoint))
                    .header("Content-Type", "application/x-www-form-urlencoded")
                    .POST(HttpRequest.BodyPublishers.ofString(form))
                    .build();

            HttpResponse<String> response = httpClient.send(request, HttpResponse.BodyHandlers.ofString());
            if (response.statusCode() != 200) {
                throw new RuntimeException("Failed to acquire Azure AD OAuth token: " + response.body());
            }

            JsonNode node = objectMapper.readTree(response.body());
            this.cachedAccessToken = node.get("access_token").asText();
            int expiresIn = node.has("expires_in") ? node.get("expires_in").asInt() : 3600;
            this.tokenExpiryEpochMs = now + (expiresIn * 1000L);
            log.info("Acquired fresh Azure AD access token, expires in {}s", expiresIn);
            return cachedAccessToken;
        } catch (Exception e) {
            throw new RuntimeException("Azure AD Token Acquisition Error: " + e.getMessage(), e);
        }
    }

    private HttpResponse<String> executeGraphRequest(String url) {
        try {
            URI uri = URI.create(url.contains(" ") ? url.replace(" ", "%20") : url);
            HttpRequest request = HttpRequest.newBuilder()
                    .uri(uri)
                    .header("Authorization", "Bearer " + getValidAccessToken())
                    .header("Accept", "application/json")
                    .GET()
                    .build();

            HttpResponse<String> response = httpClient.send(request, HttpResponse.BodyHandlers.ofString());

            if (response.statusCode() == 429 || response.statusCode() == 503) {
                long retryAfterSeconds = response.headers().firstValue("Retry-After")
                        .map(Long::parseLong)
                        .orElse(10L);
                log.warn("Microsoft Graph Throttling (HTTP {}) on URL [{}] -> Retry-After: {}s",
                        response.statusCode(), url, retryAfterSeconds);
                throw new GraphThrottlingException(retryAfterSeconds, "Microsoft Graph rate limit exceeded");
            }

            if (response.statusCode() < 200 || response.statusCode() >= 300) {
                throw new RuntimeException("Graph API call failed [" + response.statusCode() + "]: " + response.body());
            }

            return response;
        } catch (GraphThrottlingException gte) {
            throw gte;
        } catch (Exception e) {
            throw new RuntimeException("Graph Request Failed: " + e.getMessage(), e);
        }
    }

    @Override
    public List<MonitoredDrive> discoverSharePointDrives(String tenantId) {
        List<MonitoredDrive> drives = new ArrayList<>();
        String url = graphBaseUrl + "/sites/getAllSites?$select=id,name,webUrl";
        HttpResponse<String> response = executeGraphRequest(url);

        try {
            JsonNode root = objectMapper.readTree(response.body());
            JsonNode value = root.get("value");
            if (value != null && value.isArray()) {
                for (JsonNode site : value) {
                    String siteId = site.get("id").asText();
                    String siteName = site.has("name") ? site.get("name").asText() : site.get("webUrl").asText();

                    // Query drives within this site
                    String drivesUrl = graphBaseUrl + "/sites/" + siteId + "/drives?$select=id,name,driveType";
                    try {
                        HttpResponse<String> drivesResp = executeGraphRequest(drivesUrl);
                        JsonNode drivesNode = objectMapper.readTree(drivesResp.body()).get("value");
                        if (drivesNode != null && drivesNode.isArray()) {
                            for (JsonNode d : drivesNode) {
                                String driveId = d.get("id").asText();
                                String driveName = d.get("name").asText();
                                drives.add(new MonitoredDrive(driveId, tenantId, DriveType.SHAREPOINT_LIBRARY, siteName + " / " + driveName, "site-admin", null, null));
                            }
                        }
                    } catch (Exception ex) {
                        log.warn("Could not list drives for site [{}]: {}", siteId, ex.getMessage());
                    }
                }
            }
        } catch (Exception e) {
            throw new RuntimeException("Failed parsing SharePoint sites response", e);
        }
        return drives;
    }

    @Override
    public List<MonitoredDrive> discoverUserOneDrives(String tenantId) {
        List<MonitoredDrive> drives = new ArrayList<>();
        String url = graphBaseUrl + "/users?$filter=accountEnabled eq true&$select=id,userPrincipalName";
        HttpResponse<String> response = executeGraphRequest(url);

        try {
            JsonNode root = objectMapper.readTree(response.body());
            JsonNode users = root.get("value");
            if (users != null && users.isArray()) {
                for (JsonNode user : users) {
                    String userId = user.get("id").asText();
                    String upn = user.get("userPrincipalName").asText();

                    String driveUrl = graphBaseUrl + "/users/" + userId + "/drive?$select=id,name";
                    try {
                        HttpResponse<String> driveResp = executeGraphRequest(driveUrl);
                        JsonNode d = objectMapper.readTree(driveResp.body());
                        String driveId = d.get("id").asText();
                        drives.add(new MonitoredDrive(driveId, tenantId, DriveType.USER_ONEDRIVE, upn + "'s OneDrive", upn, null, null));
                    } catch (Exception ex) {
                        log.debug("User [{}] does not have an active OneDrive provisioned: {}", upn, ex.getMessage());
                    }
                }
            }
        } catch (Exception e) {
            throw new RuntimeException("Failed parsing OneDrive users", e);
        }
        return drives;
    }

    @Override
    public DeltaSyncResult fetchDeltaChanges(String driveId, String deltaToken) {
        String url = (deltaToken != null && !deltaToken.isBlank())
                ? deltaToken
                : graphBaseUrl + "/drives/" + driveId + "/root/delta?$select=id,name,size,file,folder,parentReference,@microsoft.graph.downloadUrl";

        HttpResponse<String> response = executeGraphRequest(url);
        List<DriveItem> updatedItems = new ArrayList<>();
        List<String> deletedItemIds = new ArrayList<>();
        String nextDeltaToken = null;

        try {
            JsonNode root = objectMapper.readTree(response.body());
            JsonNode value = root.get("value");
            if (value != null && value.isArray()) {
                for (JsonNode itemNode : value) {
                    String itemId = itemNode.get("id").asText();
                    if (itemNode.has("deleted")) {
                        deletedItemIds.add(itemId);
                        continue;
                    }

                    boolean isFolder = itemNode.has("folder");
                    if (isFolder) {
                        continue; // We only scan files for permissions and malware
                    }

                    String name = itemNode.has("name") ? itemNode.get("name").asText() : itemId;
                    long size = itemNode.has("size") ? itemNode.get("size").asLong() : 0L;
                    String downloadUrl = itemNode.has("@microsoft.graph.downloadUrl")
                            ? itemNode.get("@microsoft.graph.downloadUrl").asText()
                            : graphBaseUrl + "/drives/" + driveId + "/items/" + itemId + "/content";

                    String mimeType = "application/octet-stream";
                    if (itemNode.has("file") && itemNode.get("file").has("mimeType")) {
                        mimeType = itemNode.get("file").get("mimeType").asText();
                    }

                    String path = "/";
                    if (itemNode.has("parentReference") && itemNode.get("parentReference").has("path")) {
                        path = itemNode.get("parentReference").get("path").asText();
                    }

                    updatedItems.add(new DriveItem(
                            itemId, driveId, name, path + "/" + name, size, mimeType,
                            itemNode.has("webUrl") ? itemNode.get("webUrl").asText() : "",
                            downloadUrl, false,
                            itemNode.has("eTag") ? itemNode.get("eTag").asText() : "",
                            itemNode.has("cTag") ? itemNode.get("cTag").asText() : ""
                    ));
                }
            }

            if (root.has("@odata.deltaLink")) {
                nextDeltaToken = root.get("@odata.deltaLink").asText();
            } else if (root.has("@odata.nextLink")) {
                nextDeltaToken = root.get("@odata.nextLink").asText();
            }
        } catch (Exception e) {
            throw new RuntimeException("Failed parsing delta response", e);
        }

        return new DeltaSyncResult(updatedItems, deletedItemIds, nextDeltaToken);
    }

    @Override
    public EffectiveExposure fetchPermissions(String driveId, String itemId) {
        String url = graphBaseUrl + "/drives/" + driveId + "/items/" + itemId + "/permissions";
        HttpResponse<String> response = executeGraphRequest(url);

        try {
            JsonNode root = objectMapper.readTree(response.body());
            JsonNode value = root.get("value");
            List<PermissionEntry> permissions = new ArrayList<>();
            ExposureLevel maxExposure = ExposureLevel.PRIVATE;
            boolean hasUnique = false;

            if (value != null && value.isArray()) {
                for (JsonNode permNode : value) {
                    String permId = permNode.has("id") ? permNode.get("id").asText() : UUID.randomUUID().toString();
                    boolean isInherited = permNode.has("inheritedFrom");
                    String inheritedFromId = isInherited ? permNode.get("inheritedFrom").get("id").asText() : null;
                    if (!isInherited) {
                        hasUnique = true;
                    }

                    String role = "read";
                    if (permNode.has("roles") && permNode.get("roles").isArray() && permNode.get("roles").size() > 0) {
                        role = permNode.get("roles").get(0).asText();
                    }

                    String linkScope = null;
                    if (permNode.has("link") && permNode.get("link").has("scope")) {
                        linkScope = permNode.get("link").get("scope").asText();
                        if ("anonymous".equalsIgnoreCase(linkScope)) {
                            maxExposure = ExposureLevel.PUBLIC_ANONYMOUS;
                        } else if ("organization".equalsIgnoreCase(linkScope) && maxExposure != ExposureLevel.PUBLIC_ANONYMOUS) {
                            maxExposure = ExposureLevel.ORGANIZATION_WIDE;
                        } else if ("users".equalsIgnoreCase(linkScope) && maxExposure == ExposureLevel.PRIVATE) {
                            maxExposure = ExposureLevel.EXTERNAL_GUEST;
                        }
                    }

                    String grantee = "unknown";
                    if (permNode.has("grantedToV2")) {
                        grantee = permNode.get("grantedToV2").toString();
                    }

                    permissions.add(new PermissionEntry(permId, grantee, role, isInherited, inheritedFromId, linkScope));
                }
            }

            if (maxExposure == ExposureLevel.PRIVATE && permissions.size() > 1) {
                maxExposure = ExposureLevel.INTERNAL_RESTRICTED;
            }

            return new EffectiveExposure(maxExposure, hasUnique, permissions);
        } catch (Exception e) {
            throw new RuntimeException("Failed parsing permissions", e);
        }
    }

    @Override
    public InputStream streamFileContent(String driveId, String itemId, String directDownloadUrl) {
        try {
            // Enterprise Optimization: Download via SAS URL directly from Azure Blob without passing through Graph API Gateway!
            String url = (directDownloadUrl != null && !directDownloadUrl.isBlank())
                    ? directDownloadUrl
                    : graphBaseUrl + "/drives/" + driveId + "/items/" + itemId + "/content";

            HttpRequest.Builder reqBuilder = HttpRequest.newBuilder().uri(URI.create(url)).GET();
            if (!url.contains(".blob.core.windows.net")) {
                reqBuilder.header("Authorization", "Bearer " + getValidAccessToken());
            }

            HttpResponse<InputStream> response = httpClient.send(reqBuilder.build(), HttpResponse.BodyHandlers.ofInputStream());

            if (response.statusCode() == 429 || response.statusCode() == 503) {
                long retryAfterSeconds = response.headers().firstValue("Retry-After")
                        .map(Long::parseLong)
                        .orElse(10L);
                throw new GraphThrottlingException(retryAfterSeconds, "Rate limit exceeded during download");
            }

            if (response.statusCode() != 200) {
                throw new RuntimeException("Failed downloading file content: HTTP " + response.statusCode());
            }

            return response.body();
        } catch (GraphThrottlingException gte) {
            throw gte;
        } catch (Exception e) {
            throw new RuntimeException("File streaming failed: " + e.getMessage(), e);
        }
    }
}
