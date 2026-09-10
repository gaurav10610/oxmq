# BullMQ Wire Compatibility & Cross-Language Interop

OxMQ is architected with **100% wire and protocol compatibility with BullMQ v5**.

This means you can produce jobs in TypeScript/Node.js and consume them in Java with OxMQ, or produce jobs in Java with OxMQ and consume them with BullMQ in Python or Node.js.

---

## 🔑 Redis Key Topology

OxMQ uses the exact same Redis key hierarchy and data structures as BullMQ:

| Key Pattern | Redis Type | Purpose |
| :--- | :--- | :--- |
| `bull:<queue>:wait` | List | FIFO queue of waiting job IDs ready for pickup |
| `bull:<queue>:active` | List | Currently executing job IDs |
| `bull:<queue>:delayed` | Sorted Set | Scheduled/delayed jobs scored by millisecond execution timestamp |
| `bull:<queue>:paused` | List | Jobs held while the queue is paused |
| `bull:<queue>:waiting-children` | Sorted Set | Parent DAG jobs waiting on child completion |
| `bull:<queue>:completed` | Sorted Set | Successfully finished jobs scored by timestamp |
| `bull:<queue>:failed` | Sorted Set | Failed jobs scored by failure timestamp |
| `bull:<queue>:<jobId>` | Hash | Job metadata (`name`, `data`, `opts`, `progress`, `returnvalue`, `attemptsMade`) |
| `bull:<queue>:events` | Stream | Real-time event notifications (`XADD`) |
| `bull:<queue>:limiter` | Sorted Set | Sliding window rate limiter state |

---

## 🌐 Polyglot Example: Node.js Producer $\rightarrow$ Java Consumer

### Step 1: Enqueue from BullMQ (TypeScript / Node.js)

```typescript
import { Queue } from 'bullmq';

const queue = new Queue('order-invoices', {
  connection: { host: 'localhost', port: 6379 }
});

await queue.add('generate-pdf', {
  orderId: 'ord_9901',
  customerEmail: 'enterprise@acme.com',
  amount: 4500.00
});
```

### Step 2: Process in OxMQ (Java 21 Virtual Threads)

```java
@Component
public class EnterpriseInvoiceWorker {

    @OxmqListener(queue = "order-invoices", concurrency = 50)
    public void handleInvoice(Job<InvoicePayload> job) {
        InvoicePayload payload = job.getData();
        System.out.println("Processing invoice produced by Node.js: " + payload.orderId());
    }
}
```

Because both engines share the identical BullMQ Lua scripts and Redis hashes, both run in seamless harmony on the same Redis cluster.
