# OxMQ Tracking Dashboard

> Please refer to the full [PROGRESS_TRACKER.md](PROGRESS_TRACKER.md) for the complete milestone breakdown, module matrices, and granular task checklists.

| Milestone | Scope / Goal | Status | Progress |
| :--- | :--- | :---: | :---: |
| **M1: Foundation & Wire-Compatibility** | Maven layout, Lettuce transport, Lua scripts, Models, Jackson serializer | 🟢 Done | `100%` |
| **M2: Concurrency & Virtual Threads** | Java 21 Loom dispatcher, Stalled Sentinel, Lock Extender, Exponential Backoff | 🟢 Done | `100%` |
| **M3: DAG Workflows & Rate Limiting** | `FlowProducer` DAG trees, sliding-window rate limiter, pause/resume/clean | 🟢 Done | `100%` |
| **M4: Spring Boot Integration** | Spring Boot 3.x Starter, `@EnableOxmq`, `@OxmqListener`, Actuator health | 🟢 Done | `100%` |
| **M5: Observability & Performance Metrics** | Native Micrometer telemetry (Timers, Gauges, Counters, p99 histograms) | 🟢 Done | `100%` |
| **M6: Hardening, Samples & Benchmarks** | JMH benchmark suite, Standalone & Spring Boot sample apps, Unit tests | 🟢 Done | `100%` |

*Last Updated: September 2026*
