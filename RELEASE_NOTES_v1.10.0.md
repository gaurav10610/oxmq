## 🚀 OxMQ v1.10.0: Engine Hardening, Job Scheduler Limits, Cluster Safety & Batch Reliability

OxMQ v1.10.0 is a feature and stability release providing deep engine hardening, complete recurring job scheduler limit enforcement, batch worker child failure observability, Redis cluster hash tag compliance, and robust Lua contract adherence across all 49 BullMQ scripts.

---

### 🌟 What's New & Fixed in v1.10.0

#### 1. ⏱️ Recurring Job Scheduler Limit & Lifecycle Enforcement (BUG-19)
- **Iteration Count Limits**: `JobOptions` now supports `repeatLimit`, `repeatEndDate`, `repeatStartDate`, `repeatOffset`, and `repeatTz`.
- **Producer ID Contract**: `BullScripts.updateJobScheduler` now passes `currentJobId` as `producerIdBytes`, allowing `updateJobScheduler-2.lua` to correlate the completed iteration (`ic`), enforce iteration limits, and cleanly purge schedulers that have reached their limit.
- **Worker & BatchWorker Rescheduling**: Both `OxmqWorker` and `OxmqBatchWorker` now execute `updateJobScheduler(..., job.getId())` upon job completion, guaranteeing repeated jobs terminate deterministically when the limit is reached.

#### 2. 👥 Batch Worker Child Failure Observability (BUG-20)
- Implemented `batchWorkerJobContext.getIgnoredChildrenFailures()` returning an unmodifiable list of child failures (`List<String>`), providing complete parity with single-job workers for hierarchical DAG flows.

#### 3. 🌐 Redis Cluster Hash Tag & Key Formatting Safety (BUG-21)
- Replaced manual string concatenations (`prefix + ":" + jobId`) with cluster-safe `queueKeys.toJobKey(jobId)` across worker, batch worker, and queue routines, guaranteeing proper `{prefix}` hash tag placement for Redis Cluster deployments.

#### 4. 🛡️ Redis Raw `byte[]` State Decoding Safety (BUG-22)
- Hardened `BullScripts.getState()` and `BullScripts.getStateV2()` against raw Redis `byte[]` responses, preventing accidental string-pointer formatting (`[B@...`) and ensuring accurate job state reporting.

#### 5. 🌊 FlowProducer & DAG Parent-Child Dependency Tracking (BUG-1, BUG-15)
- Fully aligned `FlowProducer` with BullMQ's native parent-child dependency contract, correctly populating `parent` metadata in child options and passing child IDs to parent jobs before submission.

#### 6. 🔁 Batch Worker Exponential Backoff & Retry Safety (BUG-7, BUG-16)
- Preserved job backoff strategies, custom delays, and retry counts across batch worker failures and retries.

#### 7. 🧹 Queue Obliterate & Virtual Group Cleanup (BUG-4, BUG-17)
- Extended `queue.obliterate()` to purge all virtual tenant group data (`groups:waiting`, `groups:active`, `groups:paused`, `groups:maxed`, and tenant hash keys), eliminating memory leaks on queue teardown.

#### 8. 🛑 CancellationToken Concurrency & Thread-Safety (BUG-5, BUG-18)
- Hardened `CancellationToken` with synchronized listener registration and state transitions, guaranteeing immediate callback execution if canceled prior to registration.

---

### 📦 Artifacts & Downloads

- `oxmq-core-1.10.0.jar`: Core pure Java queue and workflow engine with Project Loom Virtual Threads, Lettuce client, Virtual Queue Groups, and native BullMQ wire compatibility.
- `oxmq-spring-boot-starter-1.10.0.jar`: Spring Boot 3 auto-configuration starter with `@OxmqListener`, `@OxmqBatchListener`, `@EnableOxmqOutbox`, Actuator endpoints, and telemetry.
- `oxdoc-365-1.10.0.jar`: OxDoc 365 Enterprise Multi-Tenant Content Intelligence showcase application.
- `oxmq-benchmarks-1.10.0.jar`: JMH microbenchmarks and multi-tenant scale test suite.
- `mock-office365-server-1.10.0.jar`: Standalone Mock Office 365 Server with cloud chaos injection engine.
- `oxmq-spring-boot-java17-example-1.10.0.jar`: Java 17 LTS showcase application.

---

### 🚀 Getting Started

#### Maven
```xml
<dependency>
    <groupId>io.github.gaurav10610</groupId>
    <artifactId>oxmq-core</artifactId>
    <version>1.10.0</version>
</dependency>

<!-- Optional: Spring Boot 3 Starter -->
<dependency>
    <groupId>io.github.gaurav10610</groupId>
    <artifactId>oxmq-spring-boot-starter</artifactId>
    <version>1.10.0</version>
</dependency>
```

#### Gradle
```groovy
implementation("io.github.gaurav10610:oxmq-core:1.10.0")
// implementation("io.github.gaurav10610:oxmq-spring-boot-starter:1.10.0")
```

Official Documentation: https://gaurav10610.github.io/oxmq/
