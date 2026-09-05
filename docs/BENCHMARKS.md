# Performance Benchmarks & Targets

This document outlines the performance benchmarks, targets, JMH testing strategy, and low-allocation optimizations in **OxMQ**.

---

## 1. Performance Goals & Targets

| Metric | Target | Notes |
| :--- | :--- | :--- |
| **Enqueue Throughput** | $\ge 35,000$ ops/sec | Single Redis Standalone node, batch size 100 |
| **Enqueue Latency (p99)** | $\le 1.5\text{ ms}$ | Async pipelined command execution |
| **Worker Processing (I/O Bound)** | $\ge 20,000$ jobs/sec | 1,000 Virtual Threads on Java 21 |
| **State Transition Latency (p95)** | $\le 0.8\text{ ms}$ | Lua script `EVALSHA` execution |
| **Memory Footprint per Worker** | $\le 2\text{ KB}$ | Java 21 Virtual Thread baseline allocation |
| **Max Heap Allocation per 1M Jobs** | $\le 128\text{ MB}$ | Low-allocation byte-level serialization buffers |

---

## 2. Comparative Benchmark Matrix

| Feature / Metric | Quartz Scheduler | db-scheduler | JobRunr (Free) | Redisson RQueue | **OxMQ** |
| :--- | :--- | :--- | :--- | :--- | :--- |
| **Max Throughput** | ~ 400 ops/s | ~ 850 ops/s | ~ 2,200 ops/s | ~ 18,000 ops/s | **$\ge 25,000$ ops/s** |
| **Job Fetch Latency** | 1000ms - 5000ms | 1000ms - 5000ms | 1000ms - 3000ms | 1ms - 5ms | **< 1ms (Push / Stream)** |
| **Max Concurrency** | 50 - 200 threads | 50 - 200 threads | 100 - 500 threads | 500 threads | **10,000+ Virtual Threads** |
| **Lock Contention** | High (DB Row Locks) | High (DB Row Locks) | Medium (Optimistic) | Low (Redis Lock) | **Zero (Atomic Lua Scripts)** |
| **Memory per Job** | High (JDBC Connection) | High (JDBC Connection) | Medium (Object Graph)| Low | **Minimal (Loom Lightweight)**|

---

## 3. Microbenchmarking Suite (JMH)

The `oxmq-benchmarks` module includes standard JMH (Java Microbenchmark Harness) suites:

1. **`QueueEnqueueBenchmark`:** Measures single and bulk job insertion throughput with varying payload sizes (100B, 1KB, 10KB).
2. **`WorkerThroughputBenchmark`:** Measures end-to-end task execution throughput across 100, 500, 1000, and 5000 Virtual Threads.
3. **`SerializationBenchmark`:** Measures Jackson serialization and deserialization overhead for Java Records vs POJOs.
4. **`LuaExecutionBenchmark`:** Measures raw roundtrip latency of `moveToActive` and `moveToFinished` `EVALSHA` scripts.

To run benchmarks:
```bash
./mvnw clean package -pl oxmq-benchmarks
java -jar oxmq-benchmarks/target/benchmarks.jar
```
