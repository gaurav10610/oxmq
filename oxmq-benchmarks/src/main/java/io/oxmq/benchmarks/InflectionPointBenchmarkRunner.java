package io.oxmq.benchmarks;

import io.lettuce.core.RedisClient;
import io.lettuce.core.api.StatefulRedisConnection;
import io.oxmq.FlowProducer;
import io.oxmq.OxmqBatchWorker;
import io.oxmq.OxmqQueue;
import io.oxmq.OxmqWorker;
import io.oxmq.client.RedisConnectionManager;
import io.oxmq.model.FlowJob;
import io.oxmq.model.JobOptions;
import io.oxmq.model.JobRequest;
import io.oxmq.util.VirtualThreadSupport;
import java.lang.management.ManagementFactory;
import java.lang.management.ThreadMXBean;
import java.text.DecimalFormat;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.slf4j.LoggerFactory;

/**
 * Benchmark runner to determine absolute peak performance and inflection points
 * for OxMQ across Java 17 LTS (Platform Threads) and Java 21 LTS (Loom Virtual Threads).
 */
public class InflectionPointBenchmarkRunner {

    private static final String REDIS_URI = System.getProperty("redis.uri", "redis://localhost:6379");
    private static final DecimalFormat NUM_FMT = new DecimalFormat("#,##0");
    private static final DecimalFormat DEC_FMT = new DecimalFormat("#,##0.00");
    private static final ThreadMXBean THREAD_BEAN = ManagementFactory.getThreadMXBean();

    public record ConcurrencyResult(
            int concurrency,
            int totalJobs,
            long executionMs,
            double throughputJobsSec,
            int peakOsThreads,
            long peakHeapMb,
            boolean verified
    ) {}

    public record DagScaleResult(
            int childCount,
            int totalNodes,
            long enqueueMs,
            double enqueueRate,
            long executionMs,
            double resolutionRate,
            long redisMemDeltaBytes,
            double bytesPerNode,
            long peakHeapMb,
            boolean verified
    ) {}

    public record BatchScaleResult(
            int batchSize,
            int totalJobs,
            int batchesProcessed,
            long executionMs,
            double throughputJobsSec,
            boolean verified
    ) {}

    public static void main(String[] args) throws Exception {
        ch.qos.logback.classic.Logger rootLogger = (ch.qos.logback.classic.Logger) LoggerFactory.getLogger(org.slf4j.Logger.ROOT_LOGGER_NAME);
        rootLogger.setLevel(ch.qos.logback.classic.Level.WARN);

        String javaVersion = System.getProperty("java.version");
        String javaVendor = System.getProperty("java.vendor");
        boolean isLoom = VirtualThreadSupport.isSupported();

        System.out.println("====================================================================================================");
        System.out.println("  🐂 OxMQ Inflection Point & Peak Scale Benchmark");
        System.out.println("  JVM Runtime: Java " + javaVersion + " (" + javaVendor + ")");
        System.out.println("  Loom Virtual Threads Supported: " + (isLoom ? "✅ YES (User-space Loom VTs)" : "❌ NO (Fallback to OS Platform Threads)"));
        System.out.println("  Redis Endpoint: " + REDIS_URI);
        System.out.println("====================================================================================================\n");

        RedisClient redisClient = RedisClient.create(REDIS_URI);

        try (StatefulRedisConnection<String, String> connection = redisClient.connect()) {
            runWarmup(redisClient, connection);

            // =========================================================================
            // Suite 1: Worker Concurrency Sweep & Degradation Inflection Point (5,000 jobs)
            // =========================================================================
            System.out.println("\n----------------------------------------------------------------------------------------------------");
            System.out.println("  SUITE 1: Worker Concurrency Sweep (10, 25, 50, 100, 200, 400, 800)");
            System.out.println("  Measures context-switch penalties, OS thread overhead, and peak throughput saturation.");
            System.out.println("----------------------------------------------------------------------------------------------------");

            int[] concurrencyLevels = {10, 25, 50, 100, 200, 400, 800};
            List<ConcurrencyResult> concurrencyResults = new ArrayList<>();
            int jobsPerRun = 5000;

            for (int concurrency : concurrencyLevels) {
                System.out.printf(">>> Testing concurrency = %3d with %s jobs... ", concurrency, NUM_FMT.format(jobsPerRun));
                ConcurrencyResult res = runConcurrencyTest(redisClient, connection, jobsPerRun, concurrency);
                concurrencyResults.add(res);
                System.out.printf("Time: %5d ms | Throughput: %8.1f jobs/s | OS Threads: %4d | Heap: %3d MB | Verified: %s%n",
                        res.executionMs(), res.throughputJobsSec(), res.peakOsThreads(), res.peakHeapMb(), res.verified() ? "✅" : "❌");
            }

            // =========================================================================
            // Suite 2: Wide DAG Fan-Out Scale & Degradation Inflection Point (up to 10,000 nodes)
            // =========================================================================
            System.out.println("\n----------------------------------------------------------------------------------------------------");
            System.out.println("  SUITE 2: Wide DAG Fan-Out Scale (100, 500, 1,000, 2,500, 5,000, 7,500, 10,000 Children)");
            System.out.println("  Measures Redis Lua dependency counter atomicity, memory delta, and resolution throughput.");
            System.out.println("----------------------------------------------------------------------------------------------------");

            int[] dagSizes = {100, 500, 1000, 2500, 5000, 7500, 10000};
            List<DagScaleResult> dagResults = new ArrayList<>();

            for (int size : dagSizes) {
                System.out.printf(">>> Testing DAG Fan-Out: 1 Parent + %6s Children... ", NUM_FMT.format(size));
                DagScaleResult res = runDagScaleTest(redisClient, connection, size, 100);
                dagResults.add(res);
                System.out.printf("Enqueue: %5d ms (%7.1f/s) | Resolve: %5d ms (%7.1f/s) | Redis: %7.1f KB (%4d B/n) | Verified: %s%n",
                        res.enqueueMs(), res.enqueueRate(), res.executionMs(), res.resolutionRate(),
                        res.redisMemDeltaBytes() / 1024.0, (long) res.bytesPerNode(), res.verified() ? "✅" : "❌");
            }

            // =========================================================================
            // Suite 3: Batch Dequeue Size Sweep & Sweet Spot Inflection Point
            // =========================================================================
            System.out.println("\n----------------------------------------------------------------------------------------------------");
            System.out.println("  SUITE 3: Batch Dequeue Size Sweep (batchSize = 10, 25, 50, 100, 200, 500, 1000)");
            System.out.println("  Measures network round-trip amortisation vs batch serialization overhead on 10,000 jobs.");
            System.out.println("----------------------------------------------------------------------------------------------------");

            int[] batchSizes = {10, 25, 50, 100, 200, 500, 1000};
            List<BatchScaleResult> batchResults = new ArrayList<>();
            int batchTotalJobs = 10000;

            for (int bSize : batchSizes) {
                System.out.printf(">>> Testing batchSize = %4d with %s jobs... ", bSize, NUM_FMT.format(batchTotalJobs));
                BatchScaleResult res = runBatchScaleTest(redisClient, connection, batchTotalJobs, bSize);
                batchResults.add(res);
                System.out.printf("Batches: %4d | Time: %5d ms | Throughput: %8.1f jobs/s | Verified: %s%n",
                        res.batchesProcessed(), res.executionMs(), res.throughputJobsSec(), res.verified() ? "✅" : "❌");
            }

            // =========================================================================
            // Suite 4: Realistic Microservice I/O Concurrency (10ms Simulated I/O)
            // =========================================================================
            System.out.println("\n----------------------------------------------------------------------------------------------------");
            System.out.println("  SUITE 4: Realistic Microservice I/O Concurrency (10ms Simulated I/O, 1,000 jobs)");
            System.out.println("  Demonstrates OS Platform Thread explosion (Java 17) vs Loom Virtual Thread unmounting (Java 21).");
            System.out.println("----------------------------------------------------------------------------------------------------");

            int[] ioConcurrencyLevels = {10, 25, 50, 100, 200, 400, 800};
            List<ConcurrencyResult> ioResults = new ArrayList<>();
            int ioJobs = 1000;
            int simulatedIoMs = 10;

            for (int conc : ioConcurrencyLevels) {
                System.out.printf(">>> Testing I/O concurrency = %3d (simulated %dms I/O)... ", conc, simulatedIoMs);
                ConcurrencyResult res = runIoConcurrencyTest(redisClient, connection, ioJobs, conc, simulatedIoMs);
                ioResults.add(res);
                System.out.printf("Time: %5d ms | Throughput: %8.1f jobs/s | Peak OS Threads: %4d | Heap: %3d MB | Verified: %s%n",
                        res.executionMs(), res.throughputJobsSec(), res.peakOsThreads(), res.peakHeapMb(), res.verified() ? "✅" : "❌");
            }

            // =========================================================================
            // Analytical Inflection Point Summary Report
            // =========================================================================
            printAnalyticalReport(javaVersion, isLoom, concurrencyResults, dagResults, batchResults, ioResults);

        } finally {
            redisClient.shutdown();
        }
    }

    private static void runWarmup(RedisClient redisClient, StatefulRedisConnection<String, String> connection) throws Exception {
        System.out.print("Executing warmup run... ");
        connection.sync().flushdb();
        OxmqQueue<String> queue = OxmqQueue.<String>builder().name("warmup-q").redisClient(redisClient).build();
        for (int i = 0; i < 100; i++) {
            queue.add("task", "val-" + i);
        }
        CountDownLatch latch = new CountDownLatch(100);
        OxmqWorker<String> worker = OxmqWorker.<String>builder()
                .queueName("warmup-q")
                .connectionManager(new RedisConnectionManager(redisClient))
                .concurrency(10)
                .processor(j -> { latch.countDown(); return "OK"; })
                .build();
        worker.start();
        latch.await(10, TimeUnit.SECONDS);
        worker.close();
        queue.close();
        connection.sync().flushdb();
        System.gc();
        Thread.sleep(100);
        System.out.println("Warmup done.\n");
    }

    private static ConcurrencyResult runConcurrencyTest(
            RedisClient redisClient,
            StatefulRedisConnection<String, String> connection,
            int totalJobs,
            int concurrency
    ) throws Exception {
        connection.sync().flushdb();
        System.gc();
        Thread.sleep(100);

        String qName = "conc-test-" + concurrency;
        OxmqQueue<String> queue = OxmqQueue.<String>builder().name(qName).redisClient(redisClient).build();

        List<JobRequest<String>> batch = new ArrayList<>(totalJobs);
        for (int i = 1; i <= totalJobs; i++) {
            batch.add(new JobRequest<>("task-" + i, "payload-" + i, new JobOptions()));
        }
        queue.addBulk(batch);

        CountDownLatch latch = new CountDownLatch(totalJobs);
        AtomicInteger processed = new AtomicInteger(0);

        int osThreadsBefore = THREAD_BEAN.getThreadCount();

        OxmqWorker<String> worker = OxmqWorker.<String>builder()
                .queueName(qName)
                .connectionManager(new RedisConnectionManager(redisClient))
                .concurrency(concurrency)
                .useVirtualThreads(true)
                .processor(job -> {
                    processed.incrementAndGet();
                    latch.countDown();
                    return "OK";
                })
                .build();

        long start = System.nanoTime();
        worker.start();

        int peakThreads = osThreadsBefore;
        // Sample threads during execution
        while (latch.getCount() > 0) {
            int current = THREAD_BEAN.getThreadCount();
            if (current > peakThreads) {
                peakThreads = current;
            }
            if (latch.await(20, TimeUnit.MILLISECONDS)) break;
        }

        long execMs = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - start);
        double throughput = (totalJobs * 1000.0) / Math.max(1, execMs);

        worker.close();
        queue.close();
        long peakHeap = getJvmUsedMemoryMb();
        boolean verified = processed.get() == totalJobs;
        connection.sync().flushdb();

        return new ConcurrencyResult(concurrency, totalJobs, execMs, throughput, peakThreads, peakHeap, verified);
    }

    private static ConcurrencyResult runIoConcurrencyTest(
            RedisClient redisClient,
            StatefulRedisConnection<String, String> connection,
            int totalJobs,
            int concurrency,
            int simulatedIoMs
    ) throws Exception {
        connection.sync().flushdb();
        System.gc();
        Thread.sleep(100);

        String qName = "io-conc-" + concurrency;
        OxmqQueue<String> queue = OxmqQueue.<String>builder().name(qName).redisClient(redisClient).build();

        List<JobRequest<String>> batch = new ArrayList<>(totalJobs);
        for (int i = 1; i <= totalJobs; i++) {
            batch.add(new JobRequest<>("io-task-" + i, "io-payload-" + i, new JobOptions()));
        }
        queue.addBulk(batch);

        CountDownLatch latch = new CountDownLatch(totalJobs);
        AtomicInteger processed = new AtomicInteger(0);

        int osThreadsBefore = THREAD_BEAN.getThreadCount();

        OxmqWorker<String> worker = OxmqWorker.<String>builder()
                .queueName(qName)
                .connectionManager(new RedisConnectionManager(redisClient))
                .concurrency(concurrency)
                .useVirtualThreads(true)
                .processor(job -> {
                    try {
                        Thread.sleep(simulatedIoMs);
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                    }
                    processed.incrementAndGet();
                    latch.countDown();
                    return "OK";
                })
                .build();

        long start = System.nanoTime();
        worker.start();

        int peakThreads = osThreadsBefore;
        while (latch.getCount() > 0) {
            int current = THREAD_BEAN.getThreadCount();
            if (current > peakThreads) {
                peakThreads = current;
            }
            if (latch.await(20, TimeUnit.MILLISECONDS)) break;
        }

        long execMs = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - start);
        double throughput = (totalJobs * 1000.0) / Math.max(1, execMs);

        worker.close();
        queue.close();
        long peakHeap = getJvmUsedMemoryMb();
        boolean verified = processed.get() == totalJobs;
        connection.sync().flushdb();

        return new ConcurrencyResult(concurrency, totalJobs, execMs, throughput, peakThreads, peakHeap, verified);
    }

    private static DagScaleResult runDagScaleTest(
            RedisClient redisClient,
            StatefulRedisConnection<String, String> connection,
            int childCount,
            int workerConcurrency
    ) throws Exception {
        connection.sync().flushdb();
        System.gc();
        Thread.sleep(100);

        long memBefore = getRedisUsedMemory(connection);
        String parentQ = "dag-p-" + childCount;
        String childQ = "dag-c-" + childCount;

        FlowProducer producer = new FlowProducer(redisClient);
        FlowJob<Map<String, Object>> root = FlowJob.of(parentQ, "root", Map.of("expected", childCount));

        for (int i = 1; i <= childCount; i++) {
            root.addChild(FlowJob.of(childQ, "c-" + i, Map.of("id", i, "score", i * 1.2)));
        }

        int totalNodes = childCount + 1;

        long enqStart = System.nanoTime();
        producer.add(root);
        long enqMs = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - enqStart);
        double enqRate = (totalNodes * 1000.0) / Math.max(1, enqMs);

        long memAfter = getRedisUsedMemory(connection);
        long memDelta = Math.max(0, memAfter - memBefore);
        double bytesPerNode = (double) memDelta / totalNodes;

        CountDownLatch rootLatch = new CountDownLatch(1);
        AtomicInteger childrenDone = new AtomicInteger(0);
        Map<String, Object> aggregated = new ConcurrentHashMap<>();

        OxmqWorker<Map<String, Object>> childWorker = OxmqWorker.<Map<String, Object>>builder()
                .queueName(childQ)
                .connectionManager(new RedisConnectionManager(redisClient))
                .concurrency(workerConcurrency)
                .useVirtualThreads(true)
                .processor(j -> {
                    childrenDone.incrementAndGet();
                    return "DONE:" + j.getName();
                })
                .build();

        OxmqWorker<Map<String, Object>> parentWorker = OxmqWorker.<Map<String, Object>>builder()
                .queueName(parentQ)
                .connectionManager(new RedisConnectionManager(redisClient))
                .concurrency(2)
                .useVirtualThreads(true)
                .processor(j -> {
                    if (j.getChildrenValues() != null) {
                        aggregated.putAll(j.getChildrenValues());
                    }
                    rootLatch.countDown();
                    return "ROOT_OK";
                })
                .build();

        long execStart = System.nanoTime();
        childWorker.start();
        parentWorker.start();

        boolean completed = rootLatch.await(120, TimeUnit.SECONDS);
        long execMs = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - execStart);
        double resolutionRate = (totalNodes * 1000.0) / Math.max(1, execMs);

        childWorker.close();
        parentWorker.close();
        long peakHeap = getJvmUsedMemoryMb();
        boolean verified = completed && childrenDone.get() == childCount && aggregated.size() == childCount;
        connection.sync().flushdb();

        return new DagScaleResult(childCount, totalNodes, enqMs, enqRate, execMs, resolutionRate, memDelta, bytesPerNode, peakHeap, verified);
    }

    private static BatchScaleResult runBatchScaleTest(
            RedisClient redisClient,
            StatefulRedisConnection<String, String> connection,
            int totalJobs,
            int batchSize
    ) throws Exception {
        connection.sync().flushdb();
        System.gc();
        Thread.sleep(100);

        String qName = "batch-test-" + batchSize;
        OxmqQueue<String> queue = OxmqQueue.<String>builder().name(qName).redisClient(redisClient).build();

        List<JobRequest<String>> batch = new ArrayList<>(totalJobs);
        for (int i = 1; i <= totalJobs; i++) {
            batch.add(new JobRequest<>("b-task-" + i, "data-" + i, new JobOptions()));
        }
        queue.addBulk(batch);

        CountDownLatch latch = new CountDownLatch(totalJobs);
        AtomicInteger batchesDone = new AtomicInteger(0);
        AtomicInteger jobsDone = new AtomicInteger(0);

        OxmqBatchWorker<String> worker = OxmqBatchWorker.<String>builder()
                .queueName(qName)
                .batchSize(batchSize)
                .connectionManager(new RedisConnectionManager(redisClient))
                .useVirtualThreads(true)
                .processor(b -> {
                    batchesDone.incrementAndGet();
                    jobsDone.addAndGet(b.size());
                    for (int i = 0; i < b.size(); i++) {
                        latch.countDown();
                    }
                    return "BATCH_OK";
                })
                .build();

        long start = System.nanoTime();
        worker.start();

        boolean completed = latch.await(120, TimeUnit.SECONDS);
        long execMs = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - start);
        double throughput = (totalJobs * 1000.0) / Math.max(1, execMs);

        worker.close();
        queue.close();
        boolean verified = completed && jobsDone.get() == totalJobs;
        connection.sync().flushdb();

        return new BatchScaleResult(batchSize, totalJobs, batchesDone.get(), execMs, throughput, verified);
    }

    private static long getRedisUsedMemory(StatefulRedisConnection<String, String> connection) {
        String info = connection.sync().info("memory");
        Pattern p = Pattern.compile("used_memory:(\\d+)");
        Matcher m = p.matcher(info);
        if (m.find()) {
            return Long.parseLong(m.group(1));
        }
        return 0L;
    }

    private static long getJvmUsedMemoryMb() {
        Runtime rt = Runtime.getRuntime();
        return (rt.totalMemory() - rt.freeMemory()) / (1024 * 1024);
    }

    private static void printAnalyticalReport(
            String javaVersion,
            boolean isLoom,
            List<ConcurrencyResult> concResults,
            List<DagScaleResult> dagResults,
            List<BatchScaleResult> batchResults,
            List<ConcurrencyResult> ioResults
    ) {
        System.out.println("\n\n====================================================================================================");
        System.out.println("  📈 COMPREHENSIVE INFLECTION POINT & PEAK ANALYSIS: JAVA " + javaVersion + (isLoom ? " (Loom VTs)" : " (Platform Threads)"));
        System.out.println("====================================================================================================");

        // 1. Concurrency Analysis
        ConcurrencyResult peakConc = concResults.stream().max(Comparator.comparingDouble(ConcurrencyResult::throughputJobsSec)).orElseThrow();
        ConcurrencyResult degradationConc = null;
        for (int i = 0; i < concResults.size(); i++) {
            if (concResults.get(i).concurrency() > peakConc.concurrency()) {
                if (concResults.get(i).throughputJobsSec() < peakConc.throughputJobsSec() * 0.95) {
                    degradationConc = concResults.get(i);
                    break;
                }
            }
        }

        System.out.println("\n### 1. Engine Core Throughput & Worker Concurrency (0ms Simulated I/O)");
        System.out.println("| Concurrency | Execution Time | Throughput | Peak OS Threads | Peak Heap | Verified | Note |");
        System.out.println("|:-----------:|:--------------:|:----------:|:---------------:|:---------:|:--------:|:-----|");
        for (ConcurrencyResult r : concResults) {
            String note = "";
            if (r.concurrency() == peakConc.concurrency()) {
                note = "🏆 **ABSOLUTE PEAK**";
            } else if (degradationConc != null && r.concurrency() == degradationConc.concurrency()) {
                note = "⚠️ **INFLECTION POINT (Degradation)**";
            }
            System.out.printf("| %11d | %11d ms | %8.1f/s | %15d | %7d MB | %8s | %-35s |%n",
                    r.concurrency(), r.executionMs(), r.throughputJobsSec(), r.peakOsThreads(), r.peakHeapMb(),
                    r.verified() ? "✅ 100%" : "❌ FAIL", note);
        }
        System.out.println("-> Concurrency Peak: " + peakConc.concurrency() + " threads (" + DEC_FMT.format(peakConc.throughputJobsSec()) + " jobs/s)");
        if (degradationConc != null) {
            System.out.println("-> Concurrency Inflection Point: Starts degrading at " + degradationConc.concurrency() + " threads (" + DEC_FMT.format(degradationConc.throughputJobsSec()) + " jobs/s)");
        } else {
            System.out.println("-> Concurrency Inflection Point: Sustained linear throughput up to " + concResults.get(concResults.size() - 1).concurrency() + " threads");
        }

        // 2. DAG Fan-Out Analysis
        DagScaleResult peakDagIngestion = dagResults.stream().max(Comparator.comparingDouble(DagScaleResult::enqueueRate)).orElseThrow();
        DagScaleResult peakDagResolution = dagResults.stream().max(Comparator.comparingDouble(DagScaleResult::resolutionRate)).orElseThrow();

        System.out.println("\n### 2. DAG Fan-Out Scaling (1 Parent + N Children)");
        System.out.println("| Children | Total Nodes | Enqueue Time | Ingestion Rate | Resolution Time | Throughput | Redis Memory | Cost / Node |");
        System.out.println("|:--------:|:-----------:|:------------:|:--------------:|:---------------:|:----------:|:------------:|:-----------:|");
        for (DagScaleResult r : dagResults) {
            System.out.printf("| %8s | %11s | %9d ms | %11.1f/s | %12d ms | %7.1f/s | %9.1f KB | %8d B |%n",
                    NUM_FMT.format(r.childCount()), NUM_FMT.format(r.totalNodes()), r.enqueueMs(), r.enqueueRate(),
                    r.executionMs(), r.resolutionRate(), r.redisMemDeltaBytes() / 1024.0, (long) r.bytesPerNode());
        }
        System.out.println("-> Peak DAG Ingestion Rate: " + DEC_FMT.format(peakDagIngestion.enqueueRate()) + " nodes/s (at " + NUM_FMT.format(peakDagIngestion.childCount()) + " children)");
        System.out.println("-> Peak DAG Resolution Throughput: " + DEC_FMT.format(peakDagResolution.resolutionRate()) + " jobs/s (at " + NUM_FMT.format(peakDagResolution.childCount()) + " children)");

        // 3. Batch Dequeue Analysis
        BatchScaleResult peakBatch = batchResults.stream().max(Comparator.comparingDouble(BatchScaleResult::throughputJobsSec)).orElseThrow();
        BatchScaleResult degradationBatch = null;
        for (int i = 0; i < batchResults.size(); i++) {
            if (batchResults.get(i).batchSize() > peakBatch.batchSize()) {
                if (batchResults.get(i).throughputJobsSec() < peakBatch.throughputJobsSec() * 0.95) {
                    degradationBatch = batchResults.get(i);
                    break;
                }
            }
        }

        System.out.println("\n### 3. Batch Dequeue Size Optimization");
        System.out.println("| Batch Size | Total Jobs | Batches | Dequeue Time | Throughput | Note |");
        System.out.println("|:----------:|:----------:|:-------:|:------------:|:----------:|:-----|");
        for (BatchScaleResult r : batchResults) {
            String note = "";
            if (r.batchSize() == peakBatch.batchSize()) {
                note = "🏆 **SWEET SPOT (Peak)**";
            } else if (degradationBatch != null && r.batchSize() == degradationBatch.batchSize()) {
                note = "⚠️ **INFLECTION POINT (Degradation)**";
            }
            System.out.printf("| %10d | %10s | %7d | %9d ms | %7.1f/s | %-35s |%n",
                    r.batchSize(), NUM_FMT.format(r.totalJobs()), r.batchesProcessed(), r.executionMs(), r.throughputJobsSec(), note);
        }
        System.out.println("-> Optimal Batch Size Sweet Spot: " + peakBatch.batchSize() + " (" + DEC_FMT.format(peakBatch.throughputJobsSec()) + " jobs/s)");
        if (degradationBatch != null) {
            System.out.println("-> Batch Size Inflection Point: Diminishing returns/degradation starts at batchSize=" + degradationBatch.batchSize());
        }

        // 4. Realistic I/O Concurrency Analysis
        ConcurrencyResult peakIo = ioResults.stream().max(Comparator.comparingDouble(ConcurrencyResult::throughputJobsSec)).orElseThrow();
        ConcurrencyResult degradationIo = null;
        for (int i = 0; i < ioResults.size(); i++) {
            if (ioResults.get(i).concurrency() > peakIo.concurrency()) {
                if (ioResults.get(i).throughputJobsSec() < peakIo.throughputJobsSec() * 0.95) {
                    degradationIo = ioResults.get(i);
                    break;
                }
            }
        }

        System.out.println("\n### 4. Realistic Microservice I/O Concurrency (10ms I/O per task)");
        System.out.println("| Concurrency | Execution Time | Throughput | Peak OS Threads | Peak Heap | Verified | Note |");
        System.out.println("|:-----------:|:--------------:|:----------:|:---------------:|:---------:|:--------:|:-----|");
        for (ConcurrencyResult r : ioResults) {
            String note = "";
            if (r.concurrency() == peakIo.concurrency()) {
                note = "🏆 **ABSOLUTE PEAK**";
            } else if (degradationIo != null && r.concurrency() == degradationIo.concurrency()) {
                note = "⚠️ **INFLECTION POINT (Degradation)**";
            }
            System.out.printf("| %11d | %11d ms | %8.1f/s | %15d | %7d MB | %8s | %-35s |%n",
                    r.concurrency(), r.executionMs(), r.throughputJobsSec(), r.peakOsThreads(), r.peakHeapMb(),
                    r.verified() ? "✅ 100%" : "❌ FAIL", note);
        }
        System.out.println("-> I/O Concurrency Peak: " + peakIo.concurrency() + " workers (" + DEC_FMT.format(peakIo.throughputJobsSec()) + " jobs/s)");
        if (degradationIo != null) {
            System.out.println("-> I/O Inflection Point: Starts degrading at " + degradationIo.concurrency() + " workers (" + DEC_FMT.format(degradationIo.throughputJobsSec()) + " jobs/s)");
        } else {
            System.out.println("-> I/O Inflection Point: Sustained linear scaling up to " + ioResults.get(ioResults.size() - 1).concurrency() + " workers");
        }

        System.out.println("\n====================================================================================================\n");
    }
}
