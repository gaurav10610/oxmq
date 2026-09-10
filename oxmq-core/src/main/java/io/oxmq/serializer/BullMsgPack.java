package io.oxmq.serializer;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.msgpack.jackson.dataformat.MessagePackFactory;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * MessagePack serializer compatible with BullMQ Redis Lua cmsgpack.unpack.
 */
public class BullMsgPack {

    private static final ObjectMapper msgPackMapper = new ObjectMapper(new MessagePackFactory());

    private static final Map<String, String> OPTS_ENCODE_MAP = Map.of(
        "failParentOnFailure", "fpof",
        "continueParentOnFailure", "cpof",
        "ignoreDependencyOnFailure", "idof",
        "removeDependencyOnFailure", "rdof",
        "keepLogs", "kl",
        "deduplication", "de"
    );

    public static byte[] pack(Object object) {
        try {
            return msgPackMapper.writeValueAsBytes(object);
        } catch (IOException e) {
            throw new UncheckedIOException("Failed to serialize to MessagePack", e);
        }
    }

    @SuppressWarnings("unchecked")
    public static <T> T unpack(byte[] bytes, Class<T> clazz) {
        try {
            return msgPackMapper.readValue(bytes, clazz);
        } catch (IOException e) {
            throw new UncheckedIOException("Failed to deserialize MessagePack", e);
        }
    }

    /**
     * Builds BullMQ ARGV[1] msgpacked arguments array:
     * [1] key prefix (e.g. "bull:myqueue:")
     * [2] custom id (or "")
     * [3] name
     * [4] timestamp
     * [5] parentKey (or null)
     * [6] parent dependencies key (or null)
     * [7] parent {id, queueKey} (or null)
     * [8] repeat job key (or null)
     * [9] deduplication key (or null)
     */
    public static byte[] buildAddJobArgs(String keyPrefix, String customId, String name,
                                         long timestamp, String parentKey, Map<String, Object> parent,
                                         String repeatJobKey, String deduplicationId) {
        List<Object> args = new ArrayList<>(9);
        args.add(keyPrefix != null ? keyPrefix : "");
        args.add(customId != null ? customId : "");
        args.add(name != null ? name : "");
        args.add(timestamp);
        args.add(parentKey);
        args.add(parentKey != null && !parentKey.isBlank() ? parentKey + ":dependencies" : null);
        args.add(parent);
        args.add(repeatJobKey);
        args.add(deduplicationId != null && !deduplicationId.isBlank() ? keyPrefix + "de:" + deduplicationId : null);
        return pack(args);
    }

    /**
     * Encodes job options with BullMQ shortened keys.
     */
    public static Map<String, Object> encodeOpts(Map<String, Object> opts) {
        if (opts == null || opts.isEmpty()) {
            return Map.of();
        }
        Map<String, Object> encoded = new HashMap<>();
        for (Map.Entry<String, Object> entry : opts.entrySet()) {
            String key = OPTS_ENCODE_MAP.getOrDefault(entry.getKey(), entry.getKey());
            encoded.put(key, entry.getValue());
        }
        return encoded;
    }
}
