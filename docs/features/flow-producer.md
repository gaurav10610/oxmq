# FlowProducer (DAG Workflows)

OxMQ includes a first-class **`FlowProducer`** engine for orchestrating complex parent-child task trees and Directed Acyclic Graphs (DAGs).

---

## 🌲 How DAG Flows Work

In a workflow tree, a parent job cannot execute until all of its dependent child jobs have successfully finished:

<p align="center">
  <img src="/assets/oxmq-dag-workflow.gif" alt="OxMQ DAG Workflow Animation" style="border-radius: 8px; box-shadow: 0 4px 12px rgba(0,0,0,0.1); width: 100%;">
</p>

1. **Child Enqueuing**: All leaf child jobs enter their respective queues in the `WAITING` state and begin processing immediately.
2. **Parent In Waiting-Children**: The parent job is enqueued in the `WAITING_CHILDREN` sorted set with an atomic counter tracking pending children.
3. **Result Propagation**: As each child completes, its return value is recorded into the parent's `processed` hash in Redis.
4. **Parent Promotion**: When the last child finishes, BullMQ Lua scripts atomically promote the parent job to `WAITING` on its queue.
5. **Parent Execution**: The parent worker consumes the parent job and can inspect the aggregated results of all completed children!

---

## 💻 Code Example: Multi-Step Report Generation

```java
package com.example;

import io.oxmq.FlowProducer;
import io.oxmq.model.FlowJob;

import java.util.List;

public class ReportFlowExample {
    public static void main(String[] args) {
        FlowProducer flowProducer = new FlowProducer("redis://localhost:6379");

        // Define Child 1: Analytics aggregation
        FlowJob<AnalyticsPayload> fetchAnalytics = FlowJob.of(
                "analytics-worker", "fetch-analytics", new AnalyticsPayload("tenant-42", "2026-Q1"));

        // Define Child 2: Financial auditing
        FlowJob<FinancePayload> auditFinance = FlowJob.of(
                "finance-worker", "audit-finance", new FinancePayload("tenant-42", 150000.00));

        // Define Parent: Render executive PDF (depends on both children)
        FlowJob<ReportSummaryPayload> generateReport = FlowJob.of(
                "reports", "compile-executive-summary", new ReportSummaryPayload("tenant-42"))
                .addChild(fetchAnalytics)
                .addChild(auditFinance);

        // Enqueue the entire DAG atomically
        String rootJobId = flowProducer.add(generateReport);
        System.out.println("Workflow created. Root parent ID: " + rootJobId);
    }
}
```

---

## 🛡️ Failure Policies

You can customize what happens to the parent if a child job encounters an error:

- **`failParentOnFailure = true`**: If any child job fails after exhausting its retries, the parent job is immediately moved to `FAILED`.
- **`removeDependencyOnFailure = true`**: The failed child is unlinked from the parent's dependency counter, allowing the parent to still execute with partial results.

---

## ⚡ Empirical Scale Benchmarks

OxMQ's `FlowProducer` has been battle-tested with **10,000+ parallel children per parent** and **multi-tier hierarchical DAGs with 2,021 nodes** across both Java 17 and Java 21:
* Enqueued 10,001 nodes in **3.10 seconds** (~3,220 nodes/sec).
* Resolved 10,000 parallel children in **9.15 seconds** (~1,092 jobs/sec).
* Preserved 100% of child return values with zero dropped dependencies.
* Memory footprint: Asymptotically stabilizes at **~908 Bytes per node** in Redis (~8.87 MB for 10,000 nodes).

For full benchmark tables, Redis RAM sizing matrices, and cloud cost guides, see the [Scale & Performance Benchmarks Guide](/operations/benchmarks).
