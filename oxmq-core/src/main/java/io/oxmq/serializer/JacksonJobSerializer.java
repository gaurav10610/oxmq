package io.oxmq.serializer;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.datatype.jdk8.Jdk8Module;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;

/**
 * Jackson-based serializer with native support for Java 21 Records, JSR-310 dates, and JDK8 types.
 */
public class JacksonJobSerializer implements JobSerializer {

    private final ObjectMapper objectMapper;

    public JacksonJobSerializer() {
        this(createDefaultObjectMapper());
    }

    public JacksonJobSerializer(ObjectMapper objectMapper) {
        this.objectMapper = objectMapper;
    }

    public static ObjectMapper createDefaultObjectMapper() {
        ObjectMapper mapper = new ObjectMapper();
        mapper.registerModule(new JavaTimeModule());
        mapper.registerModule(new Jdk8Module());
        mapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
        mapper.configure(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS, true);
        mapper.setSerializationInclusion(JsonInclude.Include.NON_NULL);
        return mapper;
    }

    @Override
    public String serialize(Object object) {
        if (object == null) {
            return null;
        }
        if (object instanceof String str) {
            return str;
        }
        try {
            return objectMapper.writeValueAsString(object);
        } catch (JsonProcessingException e) {
            throw new RuntimeException("Failed to serialize object of type " + object.getClass().getName(), e);
        }
    }

    @Override
    @SuppressWarnings("unchecked")
    public <T> T deserialize(String json, Class<T> targetClass) {
        if (json == null || json.isBlank() || targetClass == null) {
            return null;
        }
        if (targetClass == String.class) {
            return (T) json;
        }
        try {
            return objectMapper.readValue(json, targetClass);
        } catch (JsonProcessingException e) {
            throw new RuntimeException("Failed to deserialize JSON into " + targetClass.getName() + ": " + json, e);
        }
    }

    @Override
    public <T> T deserialize(String json, TypeReference<T> typeReference) {
        if (json == null || json.isBlank() || typeReference == null) {
            return null;
        }
        try {
            return objectMapper.readValue(json, typeReference);
        } catch (JsonProcessingException e) {
            throw new RuntimeException("Failed to deserialize JSON into " + typeReference.getType().getTypeName() + ": " + json, e);
        }
    }

    public ObjectMapper getObjectMapper() {
        return objectMapper;
    }
}
