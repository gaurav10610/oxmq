# Pattern: Job Scheduling & Cron

OxMQ provides both one-shot delayed execution and continuous recurring job schedulers using BullMQ's native Redis scheduler engine.

---

## ⏱️ One-Shot Delayed Jobs

Schedule a job to run at a specific time in the future:

```java
// Execute 15 minutes from now
queue.add(
    "send-reminder-email",
    new ReminderData("user_789"),
    JobOptions.builder()
        .delay(Duration.ofMinutes(15))
        .build()
);
```

### Dynamic Rescheduling

If the schedule needs to change before the job runs:

```java
String jobId = "reminder_job_123";

// Postpone the job by another 30 minutes
queue.changeDelay(jobId, Duration.ofMinutes(30));

// Or promote it immediately to WAITING
queue.promote(jobId);
```

---

## 🔁 Recurring & Cron Schedulers

For recurring maintenance tasks (such as hourly metric rollups or midnight backups), use `upsertJobScheduler`:

### 1. Fixed Interval Recurring Jobs

```java
// Run every 10 minutes
queue.upsertJobScheduler(
    "cleanup-scheduler",
    Duration.ofMinutes(10),
    "purge-temp-files",
    new CleanupPayload("/tmp/cache"),
    JobOptions.builder().build()
);
```

### 2. Cron Expression Schedulers

```java
import io.oxmq.model.JobSchedulerOptions;
import java.time.Instant;

// Run every night at midnight (UTC)
queue.upsertJobScheduler(
    "nightly-billing-run",
    JobSchedulerOptions.builder()
        .cron("0 0 * * *")
        .startDate(Instant.now())
        .build(),
    "generate-monthly-statements",
    new BillingBatchPayload("2026-Q1")
);
```

---

## 🗑️ Removing Schedulers

When a recurring job is cancelled:

```java
boolean removed = queue.removeJobScheduler("nightly-billing-run");
if (removed) {
    System.out.println("Scheduler and next pending job removed successfully.");
}
```
BullMQ's `removeJobScheduler-3.lua` script removes both the scheduler metadata hash and the next pending job from the `delayed` sorted set atomically.

---

## 🔍 Querying Registered Schedulers

Inspect active schedulers and their next execution time:

```java
// 1. Fetch metadata and next execution timestamp
Map<String, Object> schedulerMeta = queue.getJobScheduler("nightly-billing-run");
System.out.println("Next run timestamp: " + schedulerMeta.get("nextTimestamp"));

// 2. Count total registered schedulers
long count = queue.getJobSchedulersCount();
System.out.println("Active recurring schedulers: " + count);
```

---

## 🚀 Bulk Delayed Job Promotion (`promoteJobs`)

Promote a batch of delayed jobs immediately to the waiting queue in a single atomic Redis operation:

```java
// Promote up to 100 delayed jobs to WAITING immediately
long promotedCount = queue.promoteJobs(100);
System.out.println("Promoted " + promotedCount + " jobs to waiting queue.");
```
