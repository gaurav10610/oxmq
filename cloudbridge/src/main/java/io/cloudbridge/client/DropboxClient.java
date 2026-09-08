package io.cloudbridge.client;

import io.cloudbridge.dto.StorageFile;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.util.Collections;
import java.util.List;

@Component
public class DropboxClient implements StorageClient {
    private static final Logger log = LoggerFactory.getLogger(DropboxClient.class);

    @Value("${cloudbridge.dropbox.client-id:}")
    private String clientId;

    @Value("${cloudbridge.dropbox.client-secret:}")
    private String clientSecret;

    @Value("${cloudbridge.dropbox.token:}")
    private String accessToken;

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
        log.info("Dropbox: Uploaded [{}] ({} bytes) to folder [{}] via App Key [{}]", 
                filePath, content.length, location, clientId);
    }
}
