package io.oxmq.benchmarks;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.annotations.Warmup;

@State(Scope.Benchmark)
@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.SECONDS)
@Warmup(iterations = 2, time = 1, timeUnit = TimeUnit.SECONDS)
@Measurement(iterations = 3, time = 1, timeUnit = TimeUnit.SECONDS)
@Fork(1)
public class VirtualThreadThroughputBenchmark {

    private ExecutorService virtualExecutor;

    @Setup
    public void setup() {
        virtualExecutor = Executors.newVirtualThreadPerTaskExecutor();
    }

    @TearDown
    public void tearDown() {
        virtualExecutor.shutdown();
    }

    @Benchmark
    public void benchmarkVirtualThreadDispatch() throws Exception {
        var future = virtualExecutor.submit(() -> {
            // Simulated micro-task
            return 42;
        });
        future.get();
    }
}
