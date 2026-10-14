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
import io.oxmq.model.FlowJobNode;
import redis.clients.jedis.JedisPool;

import java.util.List;

public class VideoWorkflowService {

    private final FlowProducer flowProducer;

    public VideoWorkflowService(JedisPool jedisPool) {
        this.flowProducer = new FlowProducer(jedisPool);
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

        // Atomically submit entire workflow to Redis
        return flowProducer.add(packageStream);
    }
}
```

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

---

## 🛡️ Failure Policies

You can configure what happens to the parent if a child job encounters an unrecoverable failure:

### `failParentOnFailure` (Default: `false`)
If set to `true`, failure of any child task immediately fails the parent job and aborts the remainder of the pipeline.

### `removeDependencyOnFailure` (Default: `false`)
If set to `true`, a failed child is unlinked from the parent's dependency counter. The parent will still run once all other children finish, allowing graceful degradation with partial results.

---

## ⚡ Scale & Performance Limits: 5,000+ Node Trees

OxMQ's DAG engine has been empirically tested for massive scale, deep nesting, and high worker concurrency:

| Workflow Topology | Total Nodes | Concurrency | Enqueue Time | Ingestion Rate | Resolution Time | Throughput | Redis RAM Delta | Verified |
| :--- | :---: | :---: | :---: | :---: | :---: | :---: | :---: | :---: |
| **Wide DAG (1 Parent + 100 Children)** | 101 | 100 VTs | 111 ms | 909.91/s | 296 ms | 341.22/s | 220 KB | ✅ 100% |
| **Wide DAG (1 Parent + 500 Children)** | 501 | 100 VTs | 238 ms | 2,105.04/s | 706 ms | 709.63/s | 557 KB | ✅ 100% |
| **Wide DAG (1 Parent + 1,000 Children)** | 1,001 | 100 VTs | 447 ms | 2,239.37/s | 1,121 ms | 892.95/s | 1.05 MB | ✅ 100% |
| **Wide DAG (1 Parent + 2,500 Children)** | 2,501 | 100 VTs | 834 ms | 2,998.80/s | 2,540 ms | 984.65/s | 2.58 MB | ✅ 100% |
| **Wide DAG (1 Parent + 5,000 Children)** | 5,001 | 100 VTs | 1,699 ms | 2,943.50/s | 4,932 ms | 1,013.99/s | 5.15 MB | ✅ 100% |
| **3-Tier Tree (1 Root $\rightarrow$ 20 Branches $\rightarrow$ 2,000 Leaves)** | 2,021 | 100 VTs | 661 ms | 3,057.49/s | 1,997 ms | 1,012.02/s | 2.01 MB | ✅ 100% |

### Key Architectural Guarantees at Scale
* **Zero Dropped Dependencies**: Even with 5,000 parallel children finishing asynchronously across 100 Virtual Threads, the parent dependency counter decrements atomically in Redis without race conditions.
* **Child Result Aggregation**: All 5,000 return values are preserved in `bull:<parentQueue>:<parentId>:processed` and populated directly into `parentJob.getChildrenValues()`.
* **Memory Footprint**: Each DAG node consumes **~1,016 - 1,054 Bytes** in Redis, meaning an entire 5,000-task workflow requires only **~5.15 MB** of Redis RAM.
* **Sub-Second Multi-Tier Resolution**: A 2,021-node, 3-tier hierarchy was fully enqueued in 661 ms and resolved in 1.99 seconds.

For complete capacity sizing formulas and cloud cost budgeting, see the [Operations Benchmarks & Sizing Guide](/operations/benchmarks).
