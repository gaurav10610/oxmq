# FlowProducer & DAG Workflows

OxMQ includes a first-class **`FlowProducer`** engine for orchestrating complex Directed Acyclic Graphs (DAGs) and multi-step parent-child task trees.

---

## 🌲 How DAG Workflows Work

A parent task cannot execute until **all** of its dependent child tasks have completed successfully:

<p align="center">
  <img src="/assets/oxmq-dag-workflow.gif" alt="OxMQ DAG Workflow Animation" style="border-radius: 8px; box-shadow: 0 4px 12px rgba(0,0,0,0.1); width: 100%;">
</p>

### Execution Lifecycle

1. **Atomic Insertion**: The entire workflow tree is enqueued atomically in Redis using BullMQ's `addParentJob-6.lua` and `addChildren-4.lua`.
2. **Child Processing**: Leaf child tasks are immediately placed in the `WAITING` state across their target queues and claimed by parallel workers.
3. **Parent in `WAITING_CHILDREN`**: The parent job enters the `WAITING_CHILDREN` sorted set with an internal dependency counter tracking pending children.
4. **Result Passing**: As each child job completes, its return value is stored in Redis under the parent's processed results hash: `bull:<parentQueue>:<parentId>:processed`.
5. **Parent Activation**: When the last child finishes, BullMQ Lua scripts atomically promote the parent job to `WAITING` on its queue.
6. **Parent Aggregation**: The parent worker consumes the job and can inspect the return values of all child tasks!

---

## 💻 Multi-Stage Pipeline Example: Video Transcoding

Consider a video pipeline where:
- Step 1 & 2: Transcode 720p and 1080p video streams in parallel.
- Step 3: Extract the audio track in parallel.
- Step 4 (Parent): Package the HLS playlist and notify the user once all 3 parallel tasks complete.

```java
package com.example.workflows;

import io.oxmq.FlowProducer;
import io.oxmq.model.FlowJob;

import java.util.List;

public class VideoWorkflowService {

    private final FlowProducer flowProducer;

    public VideoWorkflowService(String redisUri) {
        this.flowProducer = new FlowProducer(redisUri);
    }

    public String createTranscodingPipeline(String rawVideoUrl, String videoId) {
        // Child 1: 720p Transcoder
        FlowJob<TranscodeRequest> transcode720p = FlowJob.of(
                "transcoder-720p", "transcode-720p", new TranscodeRequest(rawVideoUrl, "720p"));

        // Child 2: 1080p Transcoder
        FlowJob<TranscodeRequest> transcode1080p = FlowJob.of(
                "transcoder-1080p", "transcode-1080p", new TranscodeRequest(rawVideoUrl, "1080p"));

        // Child 3: Audio Extractor
        FlowJob<AudioExtractRequest> extractAudio = FlowJob.of(
                "audio-extractor", "extract-aac", new AudioExtractRequest(rawVideoUrl));

        // Root Parent: Package HLS stream and notify customer
        FlowJob<PackageRequest> packageStream = FlowJob.of(
                "hls-packager", "compile-hls", new PackageRequest(videoId))
                .addChild(transcode720p)
                .addChild(transcode1080p)
                .addChild(extractAudio);

        // Option A: Atomically submit a single workflow tree to Redis
        return flowProducer.add(packageStream);
    }

    public List<String> createBatchTranscodingPipelines(List<PackageRequest> requests) {
        List<FlowJob<?>> batchTrees = requests.stream().map(req -> {
            FlowJob<TranscodeRequest> c1 = FlowJob.of("transcoder", "720p", new TranscodeRequest(req.videoId(), "720p"));
            FlowJob<TranscodeRequest> c2 = FlowJob.of("transcoder", "1080p", new TranscodeRequest(req.videoId(), "1080p"));
            return FlowJob.of("packager", "compile", req).addChild(c1).addChild(c2);
        }).toList();

        // Option B: Atomically submit multiple DAG workflow trees in 1 roundtrip!
        return flowProducer.addBulk(batchTrees);
    }
}
```

---

## 🌐 Redis Cluster DAG Workflows (`FlowProducer.forCluster`)

In multi-node Redis Clusters, Redis shards keys across 16,384 hash slots based on the CRC16 checksum of the key. When a DAG spans **multiple queue names** (e.g. `video-chunks` and `video-assembly`), parent and child keys would ordinarily map to different hash slots. 

When child tasks complete, BullMQ's Lua scripts (`moveToFinished-14.lua`) must atomically access the parent's keys (`:dependencies`, `:processed`). If the keys live on different cluster shards, Redis immediately throws:
```
CROSSSLOT Keys in request don't hash to the same slot
```

### The Solution: Shared Hash-Tag Routing
OxMQ solves this via `FlowProducer.forCluster(clusterTag, redisUri)`:

```java
// 1. Create a cluster-safe FlowProducer bound to a specific hash tag
FlowProducer clusterFlow = FlowProducer.forCluster("media-pipeline", "redis://redis-cluster:6379");

// 2. Submit the multi-queue workflow tree
clusterFlow.add(packageStream);
```

#### What happens behind the scenes:
- All parent and child keys across different queues are automatically wrapped with `{media-pipeline}`:
  - Child key: `bull:{media-pipeline}:video-chunks:wait`
  - Parent key: `bull:{media-pipeline}:video-assembly:wait`
  - Parent dependencies: `bull:{media-pipeline}:video-assembly:123:dependencies`
- Because the hash tag `{media-pipeline}` is identical across all related queues, Redis Cluster routes the entire multi-queue workflow to the **exact same CRC16 slot**, guaranteeing 100% atomic Lua execution with **zero CROSSSLOT errors**.

---

## 📥 Accessing Child Results in Parent Worker

When the parent worker executes, child results are retrieved from Redis:

```java
@Component
public class HlsPackagerWorker {

    @OxmqListener(queue = "hls-packager")
    public void compileHls(Job<PackageRequest> parentJob) {
        PackageRequest data = parentJob.getData();
        System.out.println("All children finished! Packaging video: " + data.videoId());

        // Child return values are saved in the parent's processed hash
        // bull:hls-packager:<jobId>:processed
        Map<String, Object> childResults = parentJob.getChildrenValues();
    }
}
```

> [!TIP] Zero-Overhead Lazy Evaluation
> `parentJob.getChildrenValues()` is backed by a deferred `Supplier`. Standalone jobs make **0 extra Redis roundtrips** and allocate zero hash map objects on the heap during job claiming. The `bull:<queue>:<jobId>:processed` hash is queried on-demand and cached only when `getChildrenValues()` is explicitly invoked.

---

## 🛡️ Failure Policies

You can configure what happens to the parent if a child job encounters an unrecoverable failure:

### `failParentOnFailure` (Default: `false`)
If set to `true`, failure of any child task immediately fails the parent job and aborts the remainder of the pipeline.

### `removeDependencyOnFailure` (Default: `false`)
If set to `true`, a failed child is unlinked from the parent's dependency counter. The parent will still run once all other children finish, allowing graceful degradation with partial results.

---

## 🔒 Leaf-Only Deduplication in DAGs

In BullMQ v6 and OxMQ, **deduplication is restricted exclusively to leaf jobs** in DAG workflows. 
- Attempting to set `deduplicationId` or `DeduplicationOptions` on a parent or intermediate workflow node throws `IllegalArgumentException`.
- This strict contract prevents orphaned dependencies or ambiguous workflow resolution where multiple disparate parent jobs might attempt to claim the exact same deduplicated child node.

---

## 🔄 DAG Child Auto-Reattachment on Bulk Retry

When retrying failed child tasks across your cluster, BullMQ v6's `moveJobsToWait-8.lua` atomically re-attaches retried child jobs to their parent's `:dependencies` set:

```java
// Retrying failed child jobs automatically preserves parent DAG dependencies
long retried = queue.retryJobs(RetryJobsOptions.builder()
    .state("failed")
    .count(50)
    .resetAttemptsMade(true)
    .build()
);
```

This guarantees that parent jobs in `WAITING_CHILDREN` will never resolve prematurely before all retried children finish executing.

---

## 📊 Inspecting & Managing DAG Dependencies

OxMQ exposes detailed child dependency queries and programmatic management:

```java
// 1. Query dependency counts breakdown
JobDependencyCounts counts = queue.getDependencyCounts(parentJobId);
System.out.println("Processed: " + counts.processed());
System.out.println("Unprocessed: " + counts.unprocessed());
System.out.println("Failed: " + counts.failed());
System.out.println("Ignored: " + counts.ignored());

// 2. Programmatically unlink a child dependency
queue.removeChildDependency(parentJobId, "bull:child-queue:child_101");

// 3. Remove all unstarted children of a parent job
queue.removeUnprocessedChildren(parentJobId);
```

## ⚡ Scale & Performance Limits: 10,000+ Node Trees

OxMQ's DAG engine has been empirically tested for massive scale, deep nesting, and high worker concurrency across both Java 17 and Java 21:

| Workflow Topology | Total Nodes | Concurrency | Enqueue Time | Ingestion Rate | Resolution Time | Throughput | Redis RAM Delta | Cost / Node | Verified |
| :--- | :---: | :---: | :---: | :---: | :---: | :---: | :---: | :---: | :---: |
| **Wide DAG (1 Parent + 100 Children)** | 101 | 100 VTs | 55 ms | 1,836.4/s | 176 ms | 573.9/s | 134 KB | 1,364 B | ✅ 100% |
| **Wide DAG (1 Parent + 500 Children)** | 501 | 100 VTs | 176 ms | 2,846.6/s | 538 ms | 931.2/s | 502 KB | 1,026 B | ✅ 100% |
| **Wide DAG (1 Parent + 1,000 Children)** | 1,001 | 100 VTs | 320 ms | 3,128.1/s | 950 ms | 1,053.7/s | 940 KB | 961 B | ✅ 100% |
| **Wide DAG (1 Parent + 2,500 Children)** | 2,501 | 100 VTs | 789 ms | 3,169.8/s | 2,340 ms | 1,068.8/s | 2.16 MB | 886 B | ✅ 100% |
| **Wide DAG (1 Parent + 5,000 Children)** | 5,001 | 100 VTs | 1,566 ms | 3,193.5/s | 4,596 ms | 1,088.1/s | 4.52 MB | 926 B | ✅ 100% |
| **Wide DAG (1 Parent + 10,000 Children)**| 10,001 | 100 VTs | 3,106 ms | 3,219.9/s | 9,153 ms | 1,092.6/s | 8.87 MB | 908 B | ✅ 100% |
| **3-Tier Tree (1 Root $\rightarrow$ 20 Branches $\rightarrow$ 2,000 Leaves)** | 2,021 | 100 VTs | 661 ms | 3,057.5/s | 1,997 ms | 1,012.0/s | 2.01 MB | 1,016 B | ✅ 100% |

### Key Architectural Guarantees at Scale
* **Zero Dropped Dependencies**: Even with 10,000 parallel children finishing asynchronously, the parent dependency counter decrements atomically in Redis without race conditions.
* **Child Result Aggregation**: All 10,000 return values are preserved in `bull:<parentQueue>:<parentId>:processed` and populated directly into `parentJob.getChildrenValues()`.
* **Memory Footprint**: Memory cost stabilizes at **~908 Bytes per node** in Redis, meaning an entire 10,000-task workflow requires only **~8.87 MB** of Redis RAM.
* **Sub-Second Multi-Tier Resolution**: A 2,021-node, 3-tier hierarchy was fully enqueued in 661 ms and resolved in 1.99 seconds.

For complete capacity sizing formulas and cloud cost budgeting, see the [Operations Benchmarks & Sizing Guide](/operations/benchmarks).
