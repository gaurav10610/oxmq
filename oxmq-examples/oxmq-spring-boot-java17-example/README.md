# OxMQ Java 17 Spring Boot Showcase

This module demonstrates **100% annotation-driven background job and workflow processing** using OxMQ in Spring Boot 3 on **Java 17 LTS baseline** (with automatic Virtual Thread promotion when run on Java 21+).

---

## 🌟 Key Features Demonstrated

1. **Zero-Boilerplate Setup**:
   Simply add `@EnableOxmq` on your `@SpringBootApplication`. OxMQ auto-configures Redis clients, workers, serialization, and health indicators.

2. **Template Pattern (`OxmqTemplate`)**:
   Unified Spring-idiomatic client for job enqueuing, bulk operations, flow DAGs, queue pausing/resuming, and state queries.

3. **Method-Level Listener (`@OxmqListener`)**:
   ```java
   @OxmqListener(queue = "${app.queues.invoices}", concurrencyString = "${app.concurrency.invoices}")
   public InvoiceResult processInvoice(
           InvoiceRequest request,
           @Header("jobId") String jobId,
           @Header("timestamp") long timestamp
   ) { ... }
   ```

4. **Batch Processing (`@OxmqBatchListener`)**:
   Atomic chunk ingestion with configurable batch size and timeouts:
   ```java
   @OxmqBatchListener(queue = "${app.queues.audit}", batchSize = 5)
   public void processAuditBatch(List<Job<AuditLogEvent>> batch) { ... }
   ```

5. **Multi-Method Class Listener (`@OxmqListener` + `@OxmqHandler`)**:
   Clean message routing patterned after Spring Kafka/Rabbit:
   ```java
   @Component
   @OxmqListener(queue = "notifications", concurrency = 5)
   public class MultiEventNotificationListener {
       @OxmqHandler(name = "email")
       public void dispatchEmail(NotificationEvent event) { ... }

       @OxmqHandler(name = "sms")
       public void dispatchSms(NotificationEvent event) { ... }
   }
   ```

6. **JVM Compatibility Strategy**:
   - Compiled with `--release 17`.
   - On Java 17: gracefully uses platform daemon thread pools.
   - On Java 21+: automatically activates Loom Virtual Threads for millions of concurrent lightweight jobs.

---

## 🚀 Running the Application

### Prerequisites
- Redis running on `localhost:6379` (e.g. `docker run -d -p 6379:6379 redis:alpine`)
- JDK 17+ (or JDK 21+)

### Run with Java 17
```bash
sdk use java 17.0.20-tem
mvn spring-boot:run -pl oxmq-examples/oxmq-spring-boot-java17-example
```

### Run with Java 21 (Virtual Threads)
```bash
sdk use java 21.0.0.0+35-tem
mvn spring-boot:run -pl oxmq-examples/oxmq-spring-boot-java17-example
```

### Run Tests
```bash
mvn test -pl oxmq-examples/oxmq-spring-boot-java17-example
```
