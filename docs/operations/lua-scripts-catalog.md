# Lua Scripts Catalog (49 Official BullMQ Scripts)

OxMQ executes the complete suite of **49 official BullMQ Lua scripts** directly inside Redis. This architecture guarantees single-roundtrip ACID atomicity without complex distributed two-phase commits.

---

## ⚡ Why Lua Scripts in Redis?

1. **Strict Atomicity**: Redis executes Lua scripts as a single atomic unit. No other command can execute concurrently while a script runs, eliminating race conditions when claiming jobs or extending locks.
2. **Minimal Network Overhead**: Instead of transferring entire payloads back and forth between Java and Redis to check timestamps or dependencies, the logic runs directly in the Redis memory engine.
3. **`EVALSHA` Caching**: OxMQ precomputes and caches the SHA-1 digest of each script. After the initial execution, subsequent calls execute `EVALSHA <sha1>`, transmitting only a 40-character hash over the wire.

---

## 📚 Categorized Script Inventory

### 1. Job Creation & Enqueueing
| Script Name | Purpose |
| :--- | :--- |
| `addStandardJob-9.lua` | Atomically adds a standard FIFO/LIFO job to the `wait` queue, handles deduplication buffer, and emits stream event. |
| `addDelayedJob-6.lua` | Pushes a delayed job into the `delayed` sorted set scored by execution timestamp. |
| `addPrioritizedJob-9.lua` | Adds a job into the priority sorted set (`pc` / prioritized set) scored by priority value. |
| `addParentJob-6.lua` | Adds a parent node in a DAG workflow and places it in `waiting-children`. |
| `addChildren-4.lua` | Enqueues multiple dependent child jobs and links them to their parent's dependency counter. |

### 2. Worker Lifecycle & Execution
| Script Name | Purpose |
| :--- | :--- |
| `moveToActive-11.lua` | Claims a waiting job, acquires the worker lock, updates `processedOn`, and enters `active`. |
| `moveToFinished-14.lua` | Completes or fails an active job, stores return value or error stack trace, triggers parent DAG checks, and auto-requeues buffered `keepLastIfActive` deduplicated jobs. |
| `moveToDelayed-11.lua` | Moves an active or waiting job to delayed set (supports `skipAttempt: true` for zero-penalty postponement via `DelayedException`). |
| `moveToWaitingChildren-5.lua` | Moves a job to `waiting-children` when it spawns or awaits child tasks. |
| `moveJobFromActiveToWait-2.lua` | Moves an in-flight active job back to `wait` (e.g. upon worker yield or shutdown). |
| `saveStacktrace-1.lua` | Appends a diagnostic failure stack trace directly to the job hash. |
| `retryJob-11.lua` | Moves a failed job with remaining attempts into the `delayed` set with calculated backoff. |
| `reprocessJob-9.lua` | Re-queues a failed or completed job back into `wait`, preserving priority ordering in priority zset. |
| `moveJobsToWait-8.lua` | Bulk-moves failed/completed jobs back to `wait` and automatically re-attaches child jobs to parent `:dependencies`. |

### 3. Lock Watchdog & Stalled Recovery
| Script Name | Purpose |
| :--- | :--- |
| `extendLock-2.lua` | Heartbeat command that extends the lock TTL of an active job while the worker is healthy. |
| `extendLocks-1.lua` | Bulk lock extension for multiple active jobs. |
| `moveStalledJobsToWait-9.lua` | Identifies orphaned jobs whose locks expired and moves them back to `wait`. |
| `releaseLock-1.lua` | Explicitly releases a worker lock token. |

### 4. In-Flight Job Operations & DAG Management
| Script Name | Purpose |
| :--- | :--- |
| `promote-9.lua` | Promotes a delayed job immediately to the waiting queue. |
| `changeDelay-4.lua` | Dynamically reschedules the delay timestamp of a delayed job. |
| `changePriority-7.lua` | Changes the priority score of a waiting or prioritized job. |
| `removeJob-2.lua` | Safely removes a job and optionally cascade-deletes child dependencies. |
| `updateData-1.lua` | Mutates the payload data of an existing job in-place. |
| `updateProgress-3.lua` | Atomically updates progress percentage and emits a stream progress event. |
| `addLog-2.lua` | Appends a diagnostic log entry to the job's log list. |
| `getDependencyCounts-4.lua` | Returns detailed counts of processed, unprocessed, failed, and ignored dependencies. |
| `removeChildDependency-1.lua` | Unlinks a child dependency from a parent job. |
| `removeUnprocessedChildren-2.lua` | Removes unstarted children of a parent DAG job. |

### 5. Queue Administration & Housekeeping
| Script Name | Purpose |
| :--- | :--- |
| `pause-7.lua` | Pauses or resumes the queue cluster-wide with metadata tracking. |
| `drain-5.lua` | Drains waiting and optionally delayed jobs from the queue. |
| `cleanJobsInSet-3.lua` | Purges expired completed or failed jobs older than a grace period. |
| `obliterate-2.lua` | Permanently destroys the queue and all its Redis keys. |
| `getState-8.lua` / `getStateV2-8.lua` | Queries the exact state of a job across all 8 Redis sets. |
| `getCounts-1.lua` | Returns count breakdown across all states (`waiting`, `active`, `delayed`, `failed`, etc.). |
| `getCountsPerPriority-2.lua` | Returns count breakdown partitioned by priority levels. |
| `getRanges-1.lua` | Fetches ranges of job IDs across multiple states in one roundtrip. |
| `getJobs-4.lua` | Fetches full job definitions and hashes across specified state sets with range offsets. |
| `getMetrics-1.lua` | Retrieves historical queue metrics (completed/failed count time series). |
| `getRateLimitTtl-2.lua` | Inspects remaining time-to-live (TTL) on active sliding-window rate limit buckets. |
| `isJobInList-1.lua` | High-speed atomic check whether a specific job ID is queued in a Redis list. |
| `paginate-1.lua` | Generic cursor-based and range pagination across sorted sets and lists. |
| `removeOrphanedJobs-1.lua` | Cleans up orphaned job hashes no longer present in state sets. |
| `isMaxed-2.lua` | Checks whether queue has reached its concurrency limit. |

### 6. Schedulers & Deduplication
| Script Name | Purpose |
| :--- | :--- |
| `addJobScheduler-11.lua` | Creates or updates a recurring cron/interval job scheduler. |
| `updateJobScheduler-12.lua` | Updates an existing recurring job scheduler definition. |
| `getJobScheduler-1.lua` | Fetches scheduler configuration and next execution timestamp. |
| `removeJobScheduler-3.lua` | Removes a recurring scheduler and its next pending delayed job. |
| `removeDeduplicationKey-1.lua`| Clears a custom deduplication lock key early. |

---

## 🔄 Upstream Synchronization & Optimizations (BullMQ v5 / v6 Parity)

OxMQ continuously tracks upstream BullMQ core commands and Redis optimizations:

1. **100% Java-Redis Wiring**:
   Every single one of the **49 official BullMQ Lua scripts** is fully wired into `BullScripts.java`, `QueueBackend.java`, and `RedisQueueBackend.java`, exposing typed Java APIs across the entire queue lifecycle.
2. **Upstream LPOS Deduplication Optimization (PR #4956)**:
   In `addStandardJob-9.lua`, `addDelayedJob-6.lua`, `addParentJob-6.lua`, and `addPrioritizedJob-9.lua`, OxMQ integrates upstream BullMQ's performance improvement for `keepLastIfActive` deduplication buffers. Rather than scanning the entire list via `LRANGE`, the script leverages native Redis 6.0.6+ `LPOS` (with automatic fallback to `LRANGE` for legacy compatibility). This delivers $O(1)$ to $O(M)$ lookups, reducing Redis CPU cycles during bursty job submissions with deduplication.

---

## 🙏 Attribution
All 49 official Lua scripts integrated into OxMQ are adapted directly from the open-source **[BullMQ](https://github.com/taskforcesh/bullmq)** project under the permissive **MIT License**.
