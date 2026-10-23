package io.oxmq.oxdoc.service;

import io.oxmq.oxdoc.model.IndexedDocumentRecord;
import java.io.IOException;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;
import org.springframework.web.servlet.mvc.method.annotation.SseEmitter;

/**
 * Manages Server-Sent Events (SSE) connections for real-time document indexing dashboard streaming.
 */
@Service
public class LiveMetricsService {

    private static final Logger log = LoggerFactory.getLogger(LiveMetricsService.class);
    private final List<SseEmitter> emitters = new CopyOnWriteArrayList<>();

    public SseEmitter subscribe() {
        SseEmitter emitter = new SseEmitter(180_000L); // 3 minutes timeout
        emitters.add(emitter);

        emitter.onCompletion(() -> emitters.remove(emitter));
        emitter.onTimeout(() -> emitters.remove(emitter));
        emitter.onError(e -> emitters.remove(emitter));

        try {
            emitter.send(SseEmitter.event().name("init").data("Connected to OxDoc 365 Live Telemetry Stream"));
        } catch (IOException e) {
            emitters.remove(emitter);
        }

        return emitter;
    }

    public void broadcastDocumentIndexed(IndexedDocumentRecord record) {
        broadcast("document-indexed", record);
        broadcast("file-scanned", record);
    }

    public void broadcastFileScanned(IndexedDocumentRecord record) {
        broadcastDocumentIndexed(record);
    }

    private void broadcast(String eventName, Object data) {
        List<SseEmitter> deadEmitters = new CopyOnWriteArrayList<>();
        for (SseEmitter emitter : emitters) {
            try {
                emitter.send(SseEmitter.event().name(eventName).data(data));
            } catch (Exception e) {
                deadEmitters.add(emitter);
            }
        }
        emitters.removeAll(deadEmitters);
    }
}
