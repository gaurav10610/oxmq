# 🌉 CloudBridge: Multi-Cloud Asset Sync & Backup Pipeline
### *The Production Reference Showcase Application for OxMQ*

<p align="center">
  <a href="https://github.com/gaurav10610/oxmq"><img src="https://img.shields.io/badge/Powered%20By-OxMQ%20v1.0.0-blue.svg" alt="Powered By OxMQ"></a>
  <a href="https://openjdk.org/projects/jdk/21/"><img src="https://img.shields.io/badge/Java-21%2B%20LTS-orange.svg" alt="Java 21"></a>
  <a href="https://spring.io/projects/spring-boot"><img src="https://img.shields.io/badge/Spring%20Boot-3.3.0-brightgreen.svg" alt="Spring Boot 3"></a>
  <a href="https://redis.io"><img src="https://img.shields.io/badge/Redis-7-red.svg" alt="Redis 7"></a>
</p>

---

## 📖 Overview

**CloudBridge** is a production-grade multi-cloud synchronization and backup application engineered to demonstrate the real-world power, resilience, and developer ergonomics of **OxMQ** — the Virtual Thread-native distributed message queue for Java 21.

Modern enterprise systems frequently need to ingest, transform, and mirror assets across fragmented cloud providers without overloading infrastructure. CloudBridge demonstrates how to solve this at scale:
1. **Scans file trees** from remote Git repositories (e.g. **GitHub** via REST API).
2. **Dispatches parallel child transfer jobs** via OxMQ's `FlowProducer` DAG engine.
3. **Concurrently streams file uploads** to multiple external cloud providers (**Dropbox** via API v2 and **Box** via Content API).
4. **Automatically unblocks parent aggregation task** the moment all parallel child transfers finish, compiling a cryptographically verifiable `SyncManifest`.
5. **Provides a real-time reactive Web UI** (`http://localhost:8080`) with animated progress indicators, DAG state inspection, and an embedded Bull-Board queue dashboard.

---

## 🏛️ How CloudBridge Leverages OxMQ

| OxMQ Feature | How CloudBridge Uses It | Value Delivered |
| :--- | :--- | :--- |
| **🌲 Parent-Child DAGs (`FlowProducer`)** | The parent `sync-orchestration-queue` job enters `WAITING_CHILDREN` state and only completes when all 8 child `file-transfer-queue` jobs finish. | Guaranteed pipeline atomicity and aggregated reporting without polling or complex database state machines. |
| **🧵 Java 21 Loom Virtual Threads** | Workers are configured with `@OxmqListener(concurrency = 20)` running on lightweight virtual threads (`Thread.ofVirtual()`). | High-concurrency network streaming without OS thread starvation or excessive memory footprint. |
| **📊 Real-time Progress & Telemetry** | Workers invoke `job.updateProgress(n)` during chunked HTTP uploads. | Live percentage progress bars streamed to the web UI and Bull-Board. |
| **🔄 Conflict & Versioning Handling** | Box and Dropbox clients detect HTTP 409 collisions and create new version uploads automatically. | Enterprise-grade cloud resilience with zero duplicate file corruption. |
| **🖥️ Bull-Board Parity** | 100% BullMQ wire-compatibility allows queue introspection via Bull-Board (`http://localhost:3000`). | Real-time queue inspection, manual job retry, and execution log tracing out of the box. |

---

## 🚀 Quick Start (60 Seconds)

### 1. Launch Redis & Bull-Board
From the project root:
```bash
docker compose up -d
```

### 2. Run CloudBridge
```bash
./mvnw spring-boot:run -pl oxmq-examples/cloudbridge
```
*(Or run `java -jar oxmq-examples/cloudbridge/target/cloudbridge-1.0.0.jar`)*

### 3. Open the Web Dashboard
Navigate to 👉 **`http://localhost:8080`** in your browser:
- **Select Source & Target:** (e.g. `GitHub` $\rightarrow$ `Dropbox` or `Box`).
- *(Optional)* Supply your own OAuth Access Tokens in the UI settings drawer (tokens are stored securely in browser `localStorage` and never committed or sent to logs).
- Click **"Start Sync Workflow (DAG)"**.
- Watch the live DAG execution graph animate across parallel child file transfers in real-time!

---

## 🔒 Security Posture & Token Management
- CloudBridge ships with **zero committed credentials or secrets**.
- Access tokens can be supplied via environment variables (`GITHUB_TOKEN`, `DROPBOX_TOKEN`, `BOX_TOKEN`) or input directly in the client-side UI drawer.
- If no cloud tokens are provided, CloudBridge automatically falls back to an internal high-fidelity mock transport so developers can test the full DAG queue workflow offline.

---

## 🧪 Automated Testing
Run the integration test suite verifying end-to-end DAG execution:
```bash
mvn test -pl oxmq-examples/cloudbridge
```
Tests verify that:
- The Spring Boot 3 application context initializes with OxMQ autoconfiguration.
- `SyncWorkflowService` dispatches a DAG flow with 8 parallel children.
- Parent aggregation completes and aggregates return values into `SyncManifest`.

---

## 👤 Author & Maintainer

Created and architected by **[Gaurav Kumar Yadav](https://www.linkedin.com/in/gaurav-kumar-yadav-6125817a/)** ([@gaurav10610](https://github.com/gaurav10610)).
