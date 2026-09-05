package io.oxmq.serializer;

import com.fasterxml.jackson.core.type.TypeReference;

/**
 * Interface for serializing and deserializing job payloads and options.
 */
public interface JobSerializer {

    /**
     * Serializes an object to JSON string.
     */
    String serialize(Object object);

    /**
     * Deserializes a JSON string to target class type.
     */
    <T> T deserialize(String json, Class<T> targetClass);

    /**
     * Deserializes a JSON string to target generic TypeReference.
     */
    <T> T deserialize(String json, TypeReference<T> typeReference);
}
