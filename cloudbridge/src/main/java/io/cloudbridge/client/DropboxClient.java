package io.cloudbridge.client;

import io.cloudbridge.dto.StorageFile;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.Collections;
import java.util.List;

@Component
public class DropboxClient implements StorageClient {
    private static final Logger log = LoggerFactory.getLogger(DropboxClient.class);

    @Value("${cloudbridge.dropbox.client-id:}")
    private String clientId;

    @Value("${cloudbridge.dropbox.client-secret:}")
    private String clientSecret;

    @Value("${cloudbridge.dropbox.token:${DROPBOX_TOKEN:}}")
    private String accessToken;

    private volatile String runtimeToken;

    public void setRuntimeToken(String token) {
        this.runtimeToken = token;
    }

    public String resolveToken() {
        if (runtimeToken != null && !runtimeToken.isBlank()) return runtimeToken;
        if (accessToken != null && !accessToken.isBlank()) return accessToken;
        String env = System.getenv("DROPBOX_TOKEN");
        return (env != null && !env.isBlank()) ? env : null;
    }

    @Override
    public String getProviderName() {
        return "dropbox";
    }

    @Override
    public List<StorageFile> listFiles(String location) {
        log.info("Scanning Dropbox folder: {} (App Key: {})", location, clientId);
        return Collections.emptyList();
    }

    @Override
    public byte[] download(String location, String filePath) {
        return new byte[0];
    }

    @Override
    public void upload(String location, String filePath, byte[] content, String mimeType) {
        String token = resolveToken();
        if (token == null || token.isBlank()) {
            log.warn("Dropbox: No user access token configured (set DROPBOX_TOKEN or pass in UI). Simulating upload for [{}] ({} bytes) to folder [{}]", 
                    filePath, content.length, location);
            return;
        }

        try {
            String folder = location.startsWith("dropbox:") ? location.substring("dropbox:".length()) : location;
            if (!folder.startsWith("/")) folder = "/" + folder;
            if (folder.endsWith("/")) folder = folder.substring(0, folder.length() - 1);
            String fullPath = folder + "/" + (filePath.startsWith("/") ? filePath.substring(1) : filePath);

            String apiArg = String.format("{\"path\":\"%s\",\"mode\":\"overwrite\",\"autorename\":false,\"mute\":false,\"strict_conflict\":false}", fullPath);

            HttpClient client = HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(15)).build();
            HttpRequest request = HttpRequest.newBuilder()
                    .uri(URI.create("https://content.dropboxapi.com/2/files/upload"))
                    .header("Authorization", "Bearer " + token)
                    .header("Dropbox-API-Arg", apiArg)
                    .header("Content-Type", "application/octet-stream")
                    .POST(HttpRequest.BodyPublishers.ofByteArray(content))
                    .build();

            HttpResponse<String> response = client.send(request, HttpResponse.BodyHandlers.ofString());
            if (response.statusCode() >= 200 && response.statusCode() < 300) {
                log.info("Dropbox: Successfully uploaded [{}] ({} bytes) to path [{}] (HTTP {})", 
                        filePath, content.length, fullPath, response.statusCode());
            } else {
                log.error("Dropbox: Upload failed for [{}] (HTTP {}): {}", filePath, response.statusCode(), response.body());
                throw new RuntimeException("Dropbox API upload failed with HTTP " + response.statusCode() + ": " + response.body());
            }
        } catch (Exception e) {
            log.error("Dropbox upload error for [{}]: {}", filePath, e.getMessage(), e);
            throw new RuntimeException("Dropbox upload failed: " + e.getMessage(), e);
        }
    }
}
