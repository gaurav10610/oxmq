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

    public FlowJobNode createTranscodingPipeline(String rawVideoUrl, String videoId) {
        // Child 1: 720p Transcoder
        FlowJob<TranscodeRequest> transcode720p = FlowJob.<TranscodeRequest>builder()
                .queueName("transcoder-720p")
                .name("transcode-720p")
                .data(new TranscodeRequest(rawVideoUrl, "720p"))
                .build();

        // Child 2: 1080p Transcoder
        FlowJob<TranscodeRequest> transcode1080p = FlowJob.<TranscodeRequest>builder()
                .queueName("transcoder-1080p")
                .name("transcode-1080p")
                .data(new TranscodeRequest(rawVideoUrl, "1080p"))
                .build();

        // Child 3: Audio Extractor
        FlowJob<AudioExtractRequest> extractAudio = FlowJob.<AudioExtractRequest>builder()
                .queueName("audio-extractor")
                .name("extract-aac")
                .data(new AudioExtractRequest(rawVideoUrl))
                .build();

        // Root Parent: Package HLS stream and notify customer
        FlowJob<PackageRequest> packageStream = FlowJob.<PackageRequest>builder()
                .queueName("hls-packager")
                .name("compile-hls")
                .data(new PackageRequest(videoId))
                .children(List.of(transcode720p, transcode1080p, extractAudio))
                .build();

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
