# Bull-Board Dashboard UI

Because OxMQ conforms 100% to BullMQ's Redis data model and key conventions, you can use the official **[Bull-Board UI](https://github.com/felixmosh/bull-board)** without writing any custom UI code.

---

## 📸 Live Bull-Board Preview with OxMQ

Here is Bull-Board actively visualizing OxMQ queues running on Java 21:

<p align="center">
  <img src="/assets/bullboard_queue.png" alt="Bull-Board Queue Monitoring" style="border-radius: 8px; box-shadow: 0 4px 12px rgba(0,0,0,0.1); width: 100%;">
</p>

### Live DAG & Parent-Child Task Inspection

OxMQ's `FlowProducer` workflow states (such as `WAITING_CHILDREN`, child task outputs, and step logs) appear natively in Bull-Board:

<p align="center">
  <img src="/assets/live_dag_completed.png" alt="Bull-Board Completed DAG Inspection" style="border-radius: 8px; box-shadow: 0 4px 12px rgba(0,0,0,0.1); width: 100%;">
</p>

---

## 🚀 Quick Docker Setup

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
