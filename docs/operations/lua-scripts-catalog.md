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
| `addStandardJob-9.lua` | Atomically adds a standard FIFO/LIFO job to the `wait` queue and emits stream event. |
| `addDelayedJob-6.lua` | Pushes a delayed job into the `delayed` sorted set scored by execution timestamp. |
| `addParentJob-6.lua` | Adds a parent node in a DAG workflow and places it in `waiting-children`. |
| `addChildren-4.lua` | Enqueues multiple dependent child jobs and links them to their parent's dependency counter. |

### 2. Worker Lifecycle & Execution
| Script Name | Purpose |
| :--- | :--- |
| `moveToActive-11.lua` | Claims a waiting job, acquires the worker lock, updates `processedOn`, and enters `active`. |
| `moveToFinished-14.lua` | Completes or fails an active job, stores return value or error stack trace, and triggers parent DAG checks. |
| `retryJob-11.lua` | Moves a failed job with remaining attempts into the `delayed` set with calculated backoff. |
| `reprocessJob-7.lua` | Re-queues a failed or completed job back into `wait` (dead-letter replay). |

### 3. Lock Watchdog & Stalled Recovery
| Script Name | Purpose |
| :--- | :--- |
| `extendLock-2.lua` | Heartbeat command that extends the lock TTL of an active job while the worker is healthy. |
| `moveStalledJobsToWait-9.lua` | Identifies orphaned jobs whose locks expired and moves them back to `wait`. |

### 4. In-Flight Job Operations
| Script Name | Purpose |
| :--- | :--- |
| `promote-9.lua` | Promotes a delayed job immediately to the waiting queue. |
| `changeDelay-4.lua` | Dynamically reschedules the delay timestamp of a delayed job. |
| `changePriority-7.lua` | Changes the priority score of a waiting job. |
| `removeJob-2.lua` | Safely removes a job and optionally cascade-deletes child dependencies. |
| `updateData-1.lua` | Mutates the payload data of an existing job in-place. |
| `updateProgress-3.lua` | Atomically updates progress percentage and emits a stream progress event. |
| `addLog-1.lua` | Appends a diagnostic log entry to the job's log list. |

### 5. Queue Administration & Housekeeping
| Script Name | Purpose |
| :--- | :--- |
| `pause-5.lua` | Pauses or resumes the queue cluster-wide. |
| `drain-5.lua` | Drains waiting and optionally delayed jobs from the queue. |
| `cleanJobsInSet-3.lua` | Purges expired completed or failed jobs older than a grace period. |
| `obliterate-3.lua` | Permanently destroys the queue and all its Redis keys. |
| `getState-8.lua` | Queries the exact state of a job across all 8 Redis sets. |
| `getCounts-1.lua` | Returns count breakdown across all states (`waiting`, `active`, `delayed`, `failed`, etc.). |

### 6. Schedulers & Deduplication
| Script Name | Purpose |
| :--- | :--- |
| `addJobScheduler-11.lua` | Creates or updates a recurring cron/interval job scheduler. |
| `removeJobScheduler-3.lua` | Removes a recurring scheduler and its next pending delayed job. |
| `removeDeduplicationKey-1.lua`| Clears a custom deduplication lock key early. |

---

## 🙏 Attribution
All 49 official Lua scripts integrated into OxMQ are adapted directly from the open-source **[BullMQ](https://github.com/taskforcesh/bullmq)** project under the permissive **MIT License**.
