package io.oxmq.serializer;

import com.fasterxml.jackson.core.type.TypeReference;
import io.oxmq.model.JobOptions;
import java.time.Instant;
import java.util.List;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

public class JacksonJobSerializerTest {

    private JacksonJobSerializer serializer;

    public record TestUserPayload(String id, String username, int score, Instant createdAt) {}

    @BeforeEach
    void setUp() {
        serializer = new JacksonJobSerializer();
    }

    @Test
    void testRecordSerializationAndDeserialization() {
        Instant now = Instant.now();
        TestUserPayload payload = new TestUserPayload("u-123", "alice", 99, now);

        String json = serializer.serialize(payload);
        assertThat(json).isNotNull().contains("alice").contains("u-123");

        TestUserPayload deserialized = serializer.deserialize(json, TestUserPayload.class);
        assertThat(deserialized).isNotNull();
        assertThat(deserialized.id()).isEqualTo("u-123");
        assertThat(deserialized.username()).isEqualTo("alice");
        assertThat(deserialized.score()).isEqualTo(99);
        assertThat(deserialized.createdAt()).isEqualTo(now);
    }

    @Test
    void testGenericListDeserialization() {
        List<String> items = List.of("apple", "banana", "cherry");
        String json = serializer.serialize(items);

        List<String> result = serializer.deserialize(json, new TypeReference<List<String>>() {});
        assertThat(result).containsExactly("apple", "banana", "cherry");
    }

    @Test
    void testJobOptionsSerialization() {
        JobOptions opts = JobOptions.builder()
                .attempts(5)
                .delayMs(5000)
                .exponentialBackoff(1000, 60000)
                .removeOnComplete(true)
                .build();

        String json = serializer.serialize(opts);
        assertThat(json).contains("\"attempts\":5").contains("\"delayMs\":5000");

        JobOptions deserialized = serializer.deserialize(json, JobOptions.class);
        assertThat(deserialized.getAttempts()).isEqualTo(5);
        assertThat(deserialized.getDelayMs()).isEqualTo(5000);
        assertThat(deserialized.isRemoveOnComplete()).isTrue();
        assertThat(deserialized.getBackoff()).isNotNull();
        assertThat(deserialized.getBackoff().calculateDelayMs(1)).isEqualTo(1000);
        assertThat(deserialized.getBackoff().calculateDelayMs(2)).isEqualTo(2000);
    }

    @Test
    void testNullHandling() {
        assertThat(serializer.serialize(null)).isNull();
        assertThat(serializer.deserialize(null, TestUserPayload.class)).isNull();
        assertThat(serializer.deserialize("", TestUserPayload.class)).isNull();
    }
}
