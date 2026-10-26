# OxMQ Custom Extension Lua Scripts

> **Architectural Separation**: All scripts in this directory (`src/main/resources/lua/extensions/`) are proprietary to OxMQ and are intentionally kept separate from the 49 upstream open-source BullMQ scripts located in `src/main/resources/lua/`.
> This guarantees that open-source BullMQ upstream Lua updates can be synchronized anytime with zero merge conflicts or regression risk.

## Script Catalog

| Script | Purpose |
| :--- | :--- |
| `addGroupJob-1.lua` | Enqueues a job into a virtual group, enforcing optional `maxSize` backlog limits and registering the group in `groups:waiting`. |
| `moveToActiveGroup-1.lua` | Dequeues the next available job across virtual groups using fair round-robin scheduling, enforcing group concurrency and rate limits. |
| `moveToActiveBatchGroup-1.lua` | Dequeues a homogeneous batch of jobs with strict Group Affinity (all jobs from the exact same group). |
| `decrementGroupActive-1.lua` | Decrements group active count upon job finish/failure, unblocking maxed groups when concurrency slots free up. |
| `pauseGroup-1.lua` | Atomically pauses a virtual group without affecting running jobs or other groups. |
| `resumeGroup-1.lua` | Atomically resumes a virtual group and restores it to `groups:waiting`. |
| `deleteGroup-1.lua` | Deletes all pending jobs in a virtual group and cleans up group indices. |
| `setGroupRateLimit-1.lua` | Applies dynamic cooldown / rate limiting to a specific virtual group. |
| `getGroupsByStatus-1.lua` | Paginates virtual groups by status (`waiting`, `limited`, `maxed`, `paused`) for Bull-Board Pro adapter compatibility. |
| `getGroupJobs-1.lua` | Returns paginated job IDs for a specific virtual group. |
| `repairGroup-1.lua` | Atomically reconciles desynchronized group active counts and restores orphaned groups from `groups:maxed` back to `groups:waiting`. |
