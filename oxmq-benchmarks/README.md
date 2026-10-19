# ⚡ OxMQ Microbenchmarks & Scale Profiling (`oxmq-benchmarks`)

<p align="center">
  <a href="https://github.com/gaurav10610/oxmq/releases/tag/v1.6.0"><img src="https://img.shields.io/badge/Release-v1.6.0-blue.svg" alt="Release v1.6.0"></a>
  <a href="https://openjdk.org/projects/jdk/17/"><img src="https://img.shields.io/badge/Java-17%2B%20%7C%2021%2B%20LTS-orange.svg" alt="Java 17+ and 21+ LTS"></a>
  <a href="https://openjdk.org/projects/loom/"><img src="https://img.shields.io/badge/Project%20Loom-Virtual%20Threads-brightgreen.svg" alt="Loom Virtual Threads"></a>
</p>

> JMH (Java Microbenchmark Harness) suites and end-to-end scale stress tests benchmarking **OxMQ on Java 17 LTS (Platform Threads) and Java 21 LTS (Project Loom Virtual Threads)** against Redis 7.

---

## 📊 Scale & DAG Benchmark Summary

### 🌳 Massive DAG Fan-Out Scaling (1 Parent + N Children up to 10,000 Nodes)

| Children | Total Nodes | Java 17 Ingestion | Java 21 Ingestion | Java 17 Resolution | Java 21 Resolution | Redis RAM Delta | Cost / Node | Verification |
| :---: | :---: | :---: | :---: | :---: | :---: | :---: | :---: | :---: |
| **100** | 101 | 1,870.4/s (54 ms) | 1,942.3/s (52 ms) | 1,383.6/s (73 ms) | 1,442.9/s (70 ms) | 148.3 KB | 1,504 B | ✅ 100% |
| **500** | 501 | 2,505.0/s (200 ms) | 2,783.3/s (180 ms) | 1,764.1/s (284 ms) | 2,236.6/s (224 ms) | 490.4 KB | 1,002 B | ✅ 100% |
| **1,000** | 1,001 | 2,780.6/s (360 ms) | 3,137.9/s (319 ms) | 2,034.6/s (492 ms) | 2,613.6/s (383 ms) | 942.8 KB | 964 B | ✅ 100% |
| **2,500** | 2,501 | 3,042.6/s (822 ms) | 3,153.8/s (793 ms) | 2,135.8/s (1,171 ms) | 2,607.9/s (959 ms) | 2.31 MB | 949 B | ✅ 100% |
| **5,000** | 5,001 | 2,985.7/s (1,675 ms) | 3,251.6/s (1,538 ms) | 2,261.9/s (2,211 ms) | 2,636.3/s (1,897 ms) | 4.61 MB | 943 B | ✅ 100% |
| **7,500** | 7,501 | 3,051.7/s (2,458 ms) | 3,238.8/s (2,316 ms) | 2,594.6/s (2,891 ms) | **2,640.3/s (2,841 ms)** | 6.63 MB | 904 B | ✅ 100% |
| **10,000** | 10,001 | **3,196.2/s (3,129 ms)** | 3,206.5/s (3,119 ms) | **2,645.1/s (3,781 ms)** | 2,329.1/s (4,294 ms) | **8.88 MB** | **909 B** | ✅ 100% |

---

## 🚀 Key Performance Insights

1. **Pure Engine Core Peak**:
   - **2,834.5 jobs/s** on Java 17 LTS (@ 400 concurrency).
   - **2,805.8 jobs/s** on Java 21 LTS (@ 400 concurrency).
   - Interning canonical Redis keys and eliminating eager `:processed` queries improved polling throughput by **~2.5x**.
2. **Batch Dequeue Peak (`OxmqBatchWorker`)**:
   - **2,957.7 jobs/s** on Java 17.
   - **2,962.1 jobs/s** on Java 21 across bulk batch drains with amortized network roundtrips.
3. **Massive DAG Resolution**:
   - Resolved a 10,001-node DAG tree (1 parent + 10,000 parallel children) in **3.78 seconds** (Java 17 @ 2,645.1 jobs/s) and **4.29 seconds** (Java 21 @ 2,329.1 jobs/s).
4. **Project Loom Concurrency Isolation (10ms Simulated Blocking I/O)**:
   - Under 800 concurrent workers performing blocking I/O, Java 21 OS thread count stays **strictly fixed at 35 threads** as virtual threads unmount in user space.
   - Java 17 platform threads scale to 68+ OS threads.
5. **Redis Memory Stabilization**:
   - Costs **~672 Bytes** per flat job.
   - Stabilizes at **~909 Bytes** per DAG tree node at scale ($\ge 2,500$ nodes), consuming just **8.88 MB** for a 10,001-node workflow tree.

---

## 🧪 Reproduction Commands

### Run JMH Benchmarks
```bash
mvn clean package -pl oxmq-benchmarks
java -jar oxmq-benchmarks/target/benchmarks.jar -f 1 -wi 3 -i 5
```

### Run DAG Scale Profiler
```bash
mvn test -pl oxmq-core -Dtest=FlowProducerScaleTest
```

For complete benchmarking architecture and methodology, see [`docs/operations/benchmarks.md`](../docs/operations/benchmarks.md).
