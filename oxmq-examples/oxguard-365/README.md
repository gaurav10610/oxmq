# OxGuard 365: Enterprise SaaS DSPM & Threat Analysis Showcase

> High-Scale Data Security Posture Management (DSPM) for Microsoft 365 (SharePoint & OneDrive) built on **Java 17 LTS** and **OxMQ**.

---

## Features Showcase
* **Parent-Child DAG Workflows (`FlowProducer`):** Each file triggers parallel permission evaluation and streaming malware inspection converging into a final aggregate verdict.
* **Distributed Token Bucket Rate Limiting:** Enforces tenant-wide rate limits across multi-pod workers via `groupKey: "tenant:" + tenantId`.
* **Approach 5 HTTP 429 Resilience:** Dynamic deferral with `DelayedException` + Redis Tenant Cooldown Sentinel to eliminate wasted HTTP requests during Microsoft Graph throttling.
* **Forward vs. Backward Scan Prioritization:** Real-time webhook events (Priority 1) strictly preempt historical baseline crawls (Priority 10), with proactive token-headroom yielding.
* **Office 365 Auto-Save Burst Debouncing:** 15s debouncing consolidates rapid-fire edits into a single scan pass.
* **Zero-Heap Streaming Malware Engine:** Calculates SHA-256, matches EICAR signatures, computes Shannon entropy, and verifies MIME magic bytes without loading files into JVM heap memory.
* **Interactive DSPM Web Console:** Built-in web UI with live metrics, exposure breakdown, threat verdicts, and real-time Server-Sent Events (SSE).

---

## Getting Started

### Prerequisites
* Java 17 LTS
* Redis running on `localhost:6379` (e.g. `docker run -d -p 6379:6379 redis:7-alpine`)

### 1. Zero-Setup Mode (Mock Engine)
OxGuard 365 includes an embedded mock engine that simulates Microsoft Graph API with realistic SharePoint sites, user OneDrives, delta sync tokens, controllable 429 bursts, and EICAR malware samples.

Run directly with Maven:
```bash
./mvnw clean spring-boot:run -pl oxmq-examples/oxguard-365
```

Open your browser at:
👉 **`http://localhost:8080`**

From the dashboard you can:
* Click **"Trigger Baseline Crawl"** to start the historical crawler (Priority 10).
* Click **"Simulate Real-Time Upload (Forward Scan)"** to trigger a high-priority event (Priority 1) with auto-save debouncing.
* Watch the live Server-Sent Events feed and see exposure badges and malware verdicts update in real time.

---

### 2. Live Mode (Microsoft Graph Developer E5 Tenant)

#### Step 1: Get a Free Microsoft 365 Developer Account
1. Sign up at the [Microsoft 365 Developer Program](https://developer.microsoft.com/en-us/microsoft-365/dev-program).
2. Choose **Configurable Sandbox** and enable the **Users pack** and **Mail/Events pack** (pre-populates 25 users and SharePoint sites).

#### Step 2: Register Azure AD App
1. Go to [Microsoft Entra Admin Center](https://entra.microsoft.com).
2. Register an app (`OxGuard-365`) and create a Client Secret.
3. Grant Application Permissions: `Sites.Read.All`, `Files.Read.All`, `User.Read.All`.
4. Click **Grant admin consent**.

#### Step 3: Configure `application.yml`
```yaml
oxguard:
  client:
    mode: LIVE
  azure:
    tenant-id: "<your-directory-tenant-id>"
    client-id: "<your-application-client-id>"
    client-secret: "<your-client-secret>"
  webhook:
    notification-url: "https://<your-ngrok-url>.ngrok-free.app/api/v1/webhooks/graph"
```

For detailed architecture, see [DESIGN.md](DESIGN.md).
