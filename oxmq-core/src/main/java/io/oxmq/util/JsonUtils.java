package io.oxmq.util;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.oxmq.serializer.JacksonJobSerializer;

/**
 * Shared utility for Jackson ObjectMapper instances.
 */
public final class JsonUtils {

    private static final ObjectMapper MAPPER = JacksonJobSerializer.createDefaultObjectMapper();

    private JsonUtils() {}

    public static ObjectMapper mapper() {
        return MAPPER;
    }
}
