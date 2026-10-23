# ☁️ Mock Office 365 Server (`mock-office365-server`)

<p align="center">
  <a href="https://github.com/gaurav10610/oxmq"><img src="https://img.shields.io/badge/Component-Cloud%20Mock-blue.svg" alt="Component Cloud Mock"></a>
  <a href="https://openjdk.org/projects/jdk/17/"><img src="https://img.shields.io/badge/Java-17%2B%20LTS-orange.svg" alt="Java 17 LTS"></a>
  <a href="https://spring.io/projects/spring-boot"><img src="https://img.shields.io/badge/Spring%20Boot-3.3.0-brightgreen.svg" alt="Spring Boot 3"></a>
</p>

> A standalone, high-fidelity mock service emulating the **Microsoft Graph v1.0 REST API**, multi-tenant licensing tiers, physical file downloads, and runtime cloud chaos injection for testing enterprise security and queue systems.

---

## 🏛️ Architecture & Endpoints

Running independently on port **`8081`**, this service decouples cloud API simulation from application business logic:

### 1. Microsoft Graph v1.0 REST APIs
* `GET /v1.0/subscribedSkus`: Returns active enterprise SKUs (`SPE_E5`, `SPE_E3`, `O365_BUSINESS_PREMIUM`) and prepaid seat counts.
* `GET /v1.0/organization`: Returns tenant organization profiles, verified domains, and display names.
* `GET /v1.0/sites/getAllSites`: Discovers SharePoint site collections.
* `GET /v1.0/sites/{siteId}/drives`: Lists document libraries within SharePoint sites.
* `GET /v1.0/users`: Lists active enterprise user principals.
* `GET /v1.0/users/{userId}/drive`: Resolves personal OneDrive document libraries.
* `GET /v1.0/drives/{driveId}/root/delta`: Change tracking delta queries with `@odata.nextLink` and final `@odata.deltaLink`.
* `GET /v1.0/drives/{driveId}/items/{itemId}/permissions`: Access Control List (ACL) with `inheritedFrom` hierarchy.
* `GET /mock-storage/download/{fileId}`: Pre-authenticated SAS streaming download (bypasses Graph API gateway).

### 2. Multi-Tier Tenant Resource Unit (RU) Emulation
Tracks burst limits matching Microsoft's official capacity bands:
* `tenant-apex-global`: **Enterprise E5** (2,000 RUs/s)
* `tenant-northwind`: **Enterprise E3** (1,200 RUs/s)
* `tenant-contoso`: **Business Standard** (600 RUs/s)
* `tenant-fabrikam`: **Business Basic** (300 RUs/s)
* `tenant-acme-smallbiz`: **Free / Dev** (100 RUs/s)

Emits standard IETF response headers: `RateLimit-Limit`, `RateLimit-Remaining`, and `RateLimit-Reset`.

---

## 💥 Cloud Chaos Injection API (`/mock-admin/chaos`)

Inject transient cloud faults and rate-limit bursts at runtime without code changes:

### 1. Inject HTTP 429 Throttle Burst with `Retry-After`
```bash
curl -X POST http://localhost:8081/mock-admin/chaos \
     -H "Content-Type: application/json" \
     -d '{
       "statusCode": 429,
       "burstCount": 25,
       "retryAfterSeconds": 3
     }'
```

### 2. Inject Transient HTTP 503 / 500 Server Errors
```bash
curl -X POST http://localhost:8081/mock-admin/chaos \
     -H "Content-Type: application/json" \
     -d '{
       "statusCode": 503,
       "burstCount": 15,
       "retryAfterSeconds": 5
     }'
```

### 3. Enable Random Error Injection Rate
```bash
# Inject 10% random HTTP 500 errors across all calls
curl -X POST "http://localhost:8081/mock-admin/chaos/random?errorRate=0.10&statusCode=500"
```

### 4. Clear All Active Chaos Rules
```bash
curl -X DELETE http://localhost:8081/mock-admin/chaos
```

---

## 📁 Real Physical File Storage

The server generates and serves real physical files on disk under `./data/m365-storage`:
- **Clean Documents**: Realistic `.docx`, `.xlsx`, `.pdf`, `.pptx` business records.
- **EICAR Test Signatures**: Standard anti-malware test signature files.
- **Ransomware Simulation**: High-entropy ($H > 7.5$) files simulating encrypted payloads.
- **MIME Mismatches**: Files claiming to be Office documents that contain PE (`MZ`) or Linux (`\x7fELF`) executable headers.

---

## 🚀 Running the Mock Server

### Standalone via Maven
```bash
mvn spring-boot:run -pl oxmq-examples/mock-office365-server
```

### Via Docker Compose (Included in OxDoc Stack)
```bash
./start-oxdoc.sh
```
Mock server is accessible at `http://localhost:8081`.
