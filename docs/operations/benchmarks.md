# JMH Benchmarks & Tuning Guide

OxMQ includes a dedicated **JMH (Java Microbenchmark Harness)** module (`oxmq-benchmarks`) designed to measure producer and consumer throughput in your own deployment environment.

---

## 🧪 Why Empirical Benchmarking Matters

In distributed message queues, real-world throughput is primarily constrained by:
1. **Network Latency**: Round-trip time (RTT) between JVM instances and the Redis server.
2. **Persistence Configuration**: Redis `appendfsync always` vs `appendfsync everysec` vs in-memory only.
3. **Payload Size**: Serializing 100-byte JSON vs 500 KB binary documents.
4. **Worker Execution Time**: How long your actual business logic holds the task.

Rather than presenting synthetic numbers from lab environments, **OxMQ empowers you to run reproducible JMH benchmarks directly on your infrastructure**.

---

## 🏃 Running the Benchmark Suite

### Step 1: Compile the Benchmark JAR
```bash
./mvnw clean test-compile -pl oxmq-benchmarks
```

### Step 2: Execute JMH Benchmarks
```bash
# Run against local Redis
java -jar oxmq-benchmarks/target/oxmq-benchmarks-1.0.0.jar

# Run with custom parameters (e.g. 50 threads, 5 warmups, 5 measurement iterations)
java -jar oxmq-benchmarks/target/oxmq-benchmarks-1.0.0.jar -t 50 -wi 5 -i 5
```

---

## ⚙️ Performance Tuning Guidelines

### 1. Connection Pool Sizing
Each concurrent worker Virtual Thread that claims or finishes a job borrows a connection from the `JedisPool`. 
- **Rule of Thumb**: Set `pool.maxTotal` to at least `2 * worker.concurrency`.
- **Default**: OxMQ configures generous pool sizes to prevent connection starvation under bursty traffic.

### 2. Linux Kernel Optimization for Redis
On production Linux hosts running Redis:
```bash
# Enable memory overcommit (prevents Redis background save failures)
sudo sysctl vm.overcommit_memory=1

# Increase TCP backlog to handle concurrent connection spikes
sudo sysctl -w net.core.somaxconn=65535
```

### 3. Pipelining Large Job Batches
When enqueuing multiple jobs in a single request (e.g. webhooks, data imports), always prefer `queue.addBulk(batch)` over looping `queue.add(...)`. Pipelining batches 100+ commands into a single TCP packet.
