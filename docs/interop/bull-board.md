# Bull-Board Dashboard UI

Because OxMQ conforms 100% to BullMQ's Redis data model and key conventions, you can use the official **[Bull-Board UI](https://github.com/felixmosh/bull-board)** without writing any custom UI code.

---

## 📸 Live Bull-Board Preview with OxMQ

Here is Bull-Board actively visualizing OxMQ queues running on Java 17+ and Java 21:

<p align="center">
  <img src="/assets/bullboard_queue.png" alt="Bull-Board Queue Monitoring" style="border-radius: 8px; box-shadow: 0 4px 12px rgba(0,0,0,0.1); width: 100%;">
</p>

### Live DAG & Parent-Child Task Inspection

OxMQ's `FlowProducer` workflow states (such as `WAITING_CHILDREN`, child task outputs, and step logs) appear natively in Bull-Board:

<p align="center">
  <img src="/assets/live_dag_completed.png" alt="Bull-Board Completed DAG Inspection" style="border-radius: 8px; box-shadow: 0 4px 12px rgba(0,0,0,0.1); width: 100%;">
</p>

---

## 🚀 Quick Docker Setup (Single Service)

To launch Bull-Board alongside Redis for your OxMQ applications:

```yaml
# docker-compose.yml
version: '3.8'

services:
  redis:
    image: redis:7-alpine
    container_name: oxmq-redis
    ports:
      - "6379:6379"

  bull-board:
    image: node:18-alpine
    container_name: oxmq-bull-board
    ports:
      - "3000:3000"
    working_dir: /app
    entrypoint: ["sh", "-c", "npx bull-board-cli --redis redis://oxmq-redis:6379 --port 3000"]
    depends_on:
      - redis
```

Run:
```bash
docker compose up -d
```

Navigate to `http://localhost:3000` to inspect live job counts, retry failed jobs, view stack traces, and pause/resume queues in real time!

---

## 🏢 Multi-Service & Multi-Tenant Bull-Board Setup

When multiple microservices share a single Redis instance using OxMQ's native context isolation (`prefix`), Bull-Board can visualize all services either in a single unified dashboard or segregated by service paths.

### Option 1: Unified Dashboard with Service Badges

By supplying the `prefix` option to BullMQ's `Queue` constructor and defining custom `displayName` labels, Bull-Board clearly groups each service's queues:

```javascript
// server.mjs
import express from 'express';
import { Queue } from 'bullmq';
import { createBullBoard } from '@bull-board/api';
import { BullMQAdapter } from '@bull-board/api/bullMQAdapter.js';
import { ExpressAdapter } from '@bull-board/express';

const connection = { host: 'localhost', port: 6379 };

const serverAdapter = new ExpressAdapter();
serverAdapter.setBasePath('/admin/queues');

// Service A: Billing
const billingInvoices = new Queue('invoices', { prefix: 'billing-service', connection });
const billingReceipts = new Queue('receipts', { prefix: 'billing-service', connection });

// Service B: Shipping
const shippingInvoices = new Queue('invoices', { prefix: 'shipping-service', connection });
const shippingLabels = new Queue('labels', { prefix: 'shipping-service', connection });

// Service C: AI Inference
const llmEmbeddings = new Queue('embeddings', { prefix: 'ai-service', connection });

createBullBoard({
  queues: [
    new BullMQAdapter(billingInvoices, { displayName: '[Billing] Invoices' }),
    new BullMQAdapter(billingReceipts, { displayName: '[Billing] Receipts' }),
    new BullMQAdapter(shippingInvoices, { displayName: '[Shipping] Invoices' }),
    new BullMQAdapter(shippingLabels, { displayName: '[Shipping] Labels' }),
    new BullMQAdapter(llmEmbeddings, { displayName: '[AI] Embeddings' }),
  ],
  serverAdapter,
});

const app = express();
app.use('/admin/queues', serverAdapter.getRouter());
app.listen(3000, () => {
  console.log('Unified Multi-Service Bull-Board: http://localhost:3000/admin/queues');
});
```

### Option 2: Automatic Microservice Discovery

If microservices dynamically spin up new queues, use this lightweight script to discover and mount all active OxMQ queues automatically:

```javascript
// auto-discover-board.mjs
import express from 'express';
import Redis from 'ioredis';
import { Queue } from 'bullmq';
import { createBullBoard } from '@bull-board/api';
import { BullMQAdapter } from '@bull-board/api/bullMQAdapter.js';
import { ExpressAdapter } from '@bull-board/express';

const redis = new Redis('redis://localhost:6379');
const connection = { host: 'localhost', port: 6379 };

async function bootstrap() {
  // Discover all registered OxMQ queues by finding their :id keys
  const idKeys = await redis.keys('*:id');
  const adapters = idKeys.map(key => {
    // Key format is "{prefix}:{queueName}:id" or "{queueName}:id"
    const parts = key.split(':');
    const prefix = parts.slice(0, -2).join(':') || 'bull';
    const queueName = parts[parts.length - 2];
    const q = new Queue(queueName, { prefix, connection });
    return new BullMQAdapter(q, { displayName: `[${prefix}] ${queueName}` });
  });

  const serverAdapter = new ExpressAdapter();
  serverAdapter.setBasePath('/admin/queues');

  createBullBoard({ queues: adapters, serverAdapter });

  const app = express();
  app.use('/admin/queues', serverAdapter.getRouter());
  app.listen(3000, () => {
    console.log(`Auto-discovered ${adapters.length} OxMQ queues. Dashboard: http://localhost:3000/admin/queues`);
  });
}

bootstrap();
```

---

## 🔐 Security & Production Access

In enterprise environments, secure your Bull-Board endpoint using HTTP Basic Auth or OAuth reverse proxy (e.g. Traefik, NGINX):

```javascript
import basicAuth from 'express-basic-auth';

app.use(
  '/admin/queues',
  basicAuth({
    users: { admin: process.env.BULL_BOARD_PASSWORD || 'secret' },
    challenge: true,
  }),
  serverAdapter.getRouter()
);
```
