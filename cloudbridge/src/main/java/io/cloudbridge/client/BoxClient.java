package io.cloudbridge.client;

import io.cloudbridge.dto.StorageFile;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.io.ByteArrayOutputStream;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Collections;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

@Component
public class BoxClient implements StorageClient {
    private static final Logger log = LoggerFactory.getLogger(BoxClient.class);
    private static final Pattern CONFLICT_ID_PATTERN = Pattern.compile("\"id\"\\s*:\\s*\"(\\d+)\"");

    @Value("${cloudbridge.box.client-id:}")
    private String clientId;

    @Value("${cloudbridge.box.client-secret:}")
    private String clientSecret;

    @Value("${cloudbridge.box.token:${BOX_TOKEN:}}")
    private String developerToken;

    private volatile String runtimeToken;

    public void setRuntimeToken(String token) {
        this.runtimeToken = token;
    }

    public String resolveToken() {
        if (runtimeToken != null && !runtimeToken.isBlank()) return runtimeToken;
        if (developerToken != null && !developerToken.isBlank()) return developerToken;
        String env = System.getenv("BOX_TOKEN");
        return (env != null && !env.isBlank()) ? env : null;
    }

    @Override
    public String getProviderName() {
        return "box";
    }

    @Override
    public List<StorageFile> listFiles(String location) {
        log.info("Scanning Box folder: {} (Client ID: {})", location, clientId);
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
            log.warn("Box: No developer/access token configured (set BOX_TOKEN or pass in UI). Simulating upload for [{}] ({} bytes) to folder [{}]", 
                    filePath, content.length, location);
            return;
        }

        try {
            String filename = filePath.contains("/") ? filePath.substring(filePath.lastIndexOf('/') + 1) : filePath;
            String folderId = "0";
            if (location.startsWith("box:")) {
                String loc = location.substring("box:".length()).replaceAll("^/+", "");
                if (loc.matches("^\\d+$")) {
                    folderId = loc;
                }
            }

            String boundary = "----BoxBoundary" + System.currentTimeMillis();
            String attributes = String.format("{\"name\":\"%s\",\"parent\":{\"id\":\"%s\"}}", filename, folderId);

            ByteArrayOutputStream baos = new ByteArrayOutputStream();
            baos.write(("--" + boundary + "\r\n").getBytes(StandardCharsets.UTF_8));
            baos.write("Content-Disposition: form-data; name=\"attributes\"\r\n\r\n".getBytes(StandardCharsets.UTF_8));
            baos.write((attributes + "\r\n").getBytes(StandardCharsets.UTF_8));

            baos.write(("--" + boundary + "\r\n").getBytes(StandardCharsets.UTF_8));
            baos.write(String.format("Content-Disposition: form-data; name=\"file\"; filename=\"%s\"\r\n", filename).getBytes(StandardCharsets.UTF_8));
            baos.write(("Content-Type: " + (mimeType != null ? mimeType : "application/octet-stream") + "\r\n\r\n").getBytes(StandardCharsets.UTF_8));
            baos.write(content);
            baos.write(("\r\n--" + boundary + "--\r\n").getBytes(StandardCharsets.UTF_8));

            HttpClient client = HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(15)).build();
            HttpRequest request = HttpRequest.newBuilder()
                    .uri(URI.create("https://upload.box.com/api/2.0/files/content"))
                    .header("Authorization", "Bearer " + token)
                    .header("Content-Type", "multipart/form-data; boundary=" + boundary)
                    .POST(HttpRequest.BodyPublishers.ofByteArray(baos.toByteArray()))
                    .build();

            HttpResponse<String> response = client.send(request, HttpResponse.BodyHandlers.ofString());
            if (response.statusCode() == 201 || response.statusCode() == 200) {
                log.info("Box: Successfully uploaded [{}] ({} bytes) to Box folder [{}] (HTTP {})", 
                        filename, content.length, folderId, response.statusCode());
            } else if (response.statusCode() == 409) {
                // File already exists - upload new version
                String existingFileId = extractFileIdFromConflict(response.body());
                if (existingFileId != null) {
                    uploadNewVersion(existingFileId, filename, content, token, mimeType, client);
                } else {
                    log.info("Box: File [{}] already exists in Box folder [{}]", filename, folderId);
                }
            } else {
                log.error("Box: Upload failed for [{}] (HTTP {}): {}", filename, response.statusCode(), response.body());
                throw new RuntimeException("Box API upload failed with HTTP " + response.statusCode() + ": " + response.body());
            }
        } catch (Exception e) {
            log.error("Box upload error for [{}]: {}", filePath, e.getMessage(), e);
            throw new RuntimeException("Box upload failed: " + e.getMessage(), e);
        }
    }

    private void uploadNewVersion(String fileId, String filename, byte[] content, String token, String mimeType, HttpClient client) throws Exception {
        String boundary = "----BoxBoundaryVer" + System.currentTimeMillis();
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        baos.write(("--" + boundary + "\r\n").getBytes(StandardCharsets.UTF_8));
        baos.write(String.format("Content-Disposition: form-data; name=\"file\"; filename=\"%s\"\r\n", filename).getBytes(StandardCharsets.UTF_8));
        baos.write(("Content-Type: " + (mimeType != null ? mimeType : "application/octet-stream") + "\r\n\r\n").getBytes(StandardCharsets.UTF_8));
        baos.write(content);
        baos.write(("\r\n--" + boundary + "--\r\n").getBytes(StandardCharsets.UTF_8));

        HttpRequest request = HttpRequest.newBuilder()
                .uri(URI.create("https://upload.box.com/api/2.0/files/" + fileId + "/content"))
                .header("Authorization", "Bearer " + token)
                .header("Content-Type", "multipart/form-data; boundary=" + boundary)
                .POST(HttpRequest.BodyPublishers.ofByteArray(baos.toByteArray()))
                .build();

        HttpResponse<String> response = client.send(request, HttpResponse.BodyHandlers.ofString());
        if (response.statusCode() == 200 || response.statusCode() == 201) {
            log.info("Box: Successfully updated new version for file ID [{}] ({}) (HTTP {})", fileId, filename, response.statusCode());
        } else {
            log.error("Box: Failed to upload new version for [{}] (HTTP {}): {}", fileId, response.statusCode(), response.body());
        }
    }

    private String extractFileIdFromConflict(String json) {
        if (json == null) return null;
        Matcher m = CONFLICT_ID_PATTERN.matcher(json);
        if (m.find()) {
            return m.group(1);
        }
        return null;
    }
}
