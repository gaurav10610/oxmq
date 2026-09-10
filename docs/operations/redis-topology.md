# Redis Key Topology & Data Structures

OxMQ strictly adheres to BullMQ v5's Redis key topology. Understanding these keys helps during cluster operations, capacity planning, and debugging with tools like `redis-cli` or Bull-Board.

---

## 🗄️ Redis Key Architecture

Assuming the default prefix `bull` and queue name `orders`:

| Redis Key | Type | Description |
| :--- | :--- | :--- |
| `bull:orders:id` | String | Atomic integer counter generating unique sequential job IDs. |
| `bull:orders:meta` | Hash | Queue metadata (e.g. paused state, custom prefix configuration). |
| `bull:orders:wait` | List | FIFO list holding IDs of jobs waiting to be claimed by workers. |
| `bull:orders:active` | List | List holding IDs of jobs currently locked and executing on workers. |
| `bull:orders:delayed` | Sorted Set | Scheduled/delayed jobs scored by millisecond execution timestamp. |
| `bull:orders:paused` | List | Jobs waiting while the queue is paused. |
| `bull:orders:waiting-children`| Sorted Set | Parent DAG jobs scored by remaining pending children count. |
| `bull:orders:completed` | Sorted Set | IDs of successfully finished jobs scored by completion timestamp. |
| `bull:orders:failed` | Sorted Set | IDs of failed jobs scored by failure timestamp. |
| `bull:orders:stalled` | Sorted Set | IDs of jobs whose worker lock expired without completion. |
| `bull:orders:<jobId>` | Hash | The primary job record (payload, options, timestamps, attempts). |
| `bull:orders:<jobId>:logs` | List | Append-only list of chronological string logs recorded for the job. |
| `bull:orders:<jobId>:processed`| Hash | In DAG workflows, stores return values from completed child jobs. |
| `bull:orders:events` | Stream | Redis stream emitting real-time events (`active`, `completed`, etc.). |
| `bull:orders:limiter` | Sorted Set | Sliding window timestamps used for rate limiting. |
| `bull:orders:repeat` | Sorted Set | Registered recurring/cron job schedulers. |

---

## 🔍 Inspecting Keys with `redis-cli`

### 1. View Waiting Jobs Count
```bash
redis-cli LLEN bull:orders:wait
```

### 2. Inspect an Individual Job Hash
```bash
redis-cli HGETALL bull:orders:101
```

Sample output:
```text
1) "name"
2) "process-payment"
3) "data"
4) "{\"orderId\":\"ord_101\",\"amount\":99.0}"
5) "opts"
6) "{\"attempts\":3,\"delay\":0}"
7) "timestamp"
8) "1773321600000"
9) "processedOn"
10) "1773321600050"
11) "attemptsMade"
12) "1"
```

### 3. Check Delayed Jobs
```bash
# Get the next 5 delayed jobs ordered by due timestamp
redis-cli ZRANGE bull:orders:delayed 0 4 WITHSCORES
```

### 4. Read Live Stream Events
```bash
# Read the last 5 events emitted by the queue
redis-cli XREVRANGE bull:orders:events + - COUNT 5
```
