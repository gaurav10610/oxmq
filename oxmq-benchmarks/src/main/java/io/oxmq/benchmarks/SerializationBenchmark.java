package io.oxmq.benchmarks;

import io.oxmq.serializer.JacksonJobSerializer;
import java.time.Instant;
import java.util.concurrent.TimeUnit;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Warmup;

@State(Scope.Benchmark)
@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.SECONDS)
@Warmup(iterations = 2, time = 1, timeUnit = TimeUnit.SECONDS)
@Measurement(iterations = 3, time = 1, timeUnit = TimeUnit.SECONDS)
@Fork(1)
public class SerializationBenchmark {

    public record BenchmarkPayload(String id, String name, double amount, Instant timestamp) {}

    private final JacksonJobSerializer serializer = new JacksonJobSerializer();
    private final BenchmarkPayload payload = new BenchmarkPayload("order-9999", "Premium Subscription", 99.99, Instant.now());
    private final String json = serializer.serialize(payload);

    @Benchmark
    public String benchmarkSerialize() {
        return serializer.serialize(payload);
    }

    @Benchmark
    public BenchmarkPayload benchmarkDeserialize() {
        return serializer.deserialize(json, BenchmarkPayload.class);
    }
}
