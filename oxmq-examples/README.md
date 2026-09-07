# 💡 OxMQ Real-World Examples & Recipes

This module contains production-grade, runnable examples demonstrating how to use **OxMQ** for real-world engineering use cases.

---

## 📂 Real-World Projects & Recipes

| Example / Recipe | Primary Class | Real-World Engineering Use Case |
| :--- | :--- | :--- |
| **1. Rate Limiting & Throttling** | [`RateLimitingExample`](src/main/java/io/oxmq/examples/RateLimitingExample.java) | Protect downstream APIs (e.g. OpenAI, Stripe, SendGrid) with strict sliding-window token-bucket rate limits across distributed workers. |
| **2. Retries, Exponential Backoff & DLQ** | [`RetriesAndDlqExample`](src/main/java/io/oxmq/examples/RetriesAndDlqExample.java) | Critical payment webhook delivery handling transient 503 errors, automatic exponential backoff with jitter, and dead-letter queue failure archiving. |
| **3. Parent-Child DAG Workflows** | [`DagWorkflowExample`](src/main/java/io/oxmq/examples/DagWorkflowExample.java) | Multi-stage video transcoding / ETL pipeline where parent assembly task automatically waits for parallel child chunk completion and receives child return values. |
| **4. Batch Dequeue & Bulk Ingestion** | [`BatchDatabaseIngestionExample`](src/main/java/io/oxmq/examples/BatchDatabaseIngestionExample.java) | High-throughput bulk popping of up to 100 jobs at once for fast batch database ingestion (ClickHouse, Elasticsearch, PostgreSQL `saveAll`). |
| **5. Scheduled Delays & Deduplication** | [`ScheduledAndDedupExample`](src/main/java/io/oxmq/examples/ScheduledAndDedupExample.java) | Abandoned cart reminders with millisecond delay + Idempotent custom `jobId` deduplication preventing duplicate executions. |
| **6. Real-Time Progress & Event Streaming**| [`ProgressAndEventsExample`](src/main/java/io/oxmq/examples/ProgressAndEventsExample.java) | Long-running data migration / AI batch processing with real-time `job.updateProgress()` (0-100%), `job.log()`, and `QueueEvents` Pub/Sub listener. |
| **7. Spring Boot 3 Declarative Webhooks** | [`SpringBootExampleApplication`](src/main/java/io/oxmq/examples/spring/SpringBootExampleApplication.java) | Full Spring Boot 3 microservice with `@OxmqListener`, rate limiting, REST trigger endpoints, and Actuator health metrics. |

---

## 🚀 How to Run Examples

Make sure a Redis instance is running locally on port `6379` (or pass `-Doxmq.redis.uri=redis://host:port`):

```bash
# Rate Limiting
./mvnw exec:java -pl oxmq-examples -Dexec.mainClass="io.oxmq.examples.RateLimitingExample"

# Retries & DLQ
./mvnw exec:java -pl oxmq-examples -Dexec.mainClass="io.oxmq.examples.RetriesAndDlqExample"

# Parent-Child DAG Workflow
./mvnw exec:java -pl oxmq-examples -Dexec.mainClass="io.oxmq.examples.DagWorkflowExample"

# Batch Database Ingestion
./mvnw exec:java -pl oxmq-examples -Dexec.mainClass="io.oxmq.examples.BatchDatabaseIngestionExample"

# Scheduled & Deduplication
./mvnw exec:java -pl oxmq-examples -Dexec.mainClass="io.oxmq.examples.ScheduledAndDedupExample"

# Real-time Progress & Events
./mvnw exec:java -pl oxmq-examples -Dexec.mainClass="io.oxmq.examples.ProgressAndEventsExample"

# Spring Boot 3 Microservice
./mvnw spring-boot:run -pl oxmq-examples -Dspring-boot.run.main-class="io.oxmq.examples.spring.SpringBootExampleApplication"
```
