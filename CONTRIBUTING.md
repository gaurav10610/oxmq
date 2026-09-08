# Contributing to OxMQ 🐂

Thank you for your interest in contributing to **OxMQ**! We welcome bug fixes, performance optimizations, documentation improvements, new showcase examples, and architectural discussions from the developer community.

---

## 🧭 Code of Conduct

All contributors and participants agree to abide by our [Code of Conduct](CODE_OF_CONDUCT.md). Please report unacceptable behavior to security@oxmq.io or via repository maintainers.

---

## 🛠️ Local Development Setup

### Prerequisites
* **Java 21 LTS** or higher (OpenJDK, Temurin, Corretto, GraalVM)
* **Maven 3.9+**
* **Docker & Docker Compose** (for running Redis 7 locally)
* **Git**

### 1. Fork & Clone Repository

```bash
git clone https://github.com/gaurav10610/oxmq.git
cd oxmq
git checkout develop
```

### 2. Start Local Redis & Observability Services

```bash
docker compose up -d
```

Verify Redis is ready:
```bash
redis-cli ping
# PONG
```

### 3. Build & Run Tests

```bash
mvn clean test
```

---

## 🏗️ Multi-Module Project Structure

OxMQ is structured into clean, decoupled Maven modules:

```
oxmq/
├── oxmq-core/                  # Core Lua scripts, connection management, Virtual Thread workers, FlowProducer
├── oxmq-spring-boot-starter/   # Spring Boot 3 auto-configuration, @OxmqListener, Actuator health
├── oxmq-benchmarks/            # JMH microbenchmarks for throughput & latency
├── cloudbridge/                # Flagship showcase app: Multi-cloud asset sync (GitHub, Dropbox, Box)
└── docs/                       # Technical architecture, guides, and comparison docs
```

---

## 📐 Coding & Architectural Standards

1. **Virtual Threads First**:
   - Always leverage Java 21 Virtual Threads (`Thread.ofVirtual()`) for I/O dispatchers.
   - Avoid `synchronized` blocks that pin carrier threads; use `ReentrantLock`, `Semaphore`, or atomic variables (`AtomicBoolean`, `AtomicLong`).

2. **Redis & Lua Script Integrity**:
   - All queue state transitions must execute atomically via Redis Lua scripts in `oxmq-core/src/main/resources/lua/`.
   - Redis keys must strictly match BullMQ v5 conventions (`bull:<queue_name>:<state>`) for polyglot compatibility.

3. **Wire Compatibility & Serialization**:
   - Payloads and options must serialize/deserialize cleanly via Jackson 2 (`JacksonJobSerializer`).
   - Support Java 21 `record` classes natively.

4. **Testing Rigor**:
   - Unit tests must be fast and deterministic.
   - Integration tests must clean up their Redis queues via `queue.obliterate()` in `@BeforeEach` or `@AfterEach`.

---

## 🔀 Branching Strategy & Git Workflow

* **`main`**: Stable, tagged production release branch.
* **`develop`**: Active development branch. All pull requests should target `develop`.
* **Feature Branches**: `feat/<feature-name>`, `fix/<bug-name>`, `docs/<topic>`.

### Conventional Commits
We follow the [Conventional Commits](https://www.conventionalcommits.org/) specification:
* `feat(core): add batch dequeue support`
* `fix(worker): prevent connection contention under high concurrency`
* `docs(readme): add job lifecycle mermaid diagram`
* `test(spring): add concurrent REST load test`

---

## 🚀 Submitting a Pull Request

1. Push your changes to your feature branch in your fork.
2. Open a Pull Request targeting the `develop` branch of `gaurav10610/oxmq`.
3. Complete the [Pull Request Template](.github/PULL_REQUEST_TEMPLATE.md).
4. Ensure all unit and integration tests pass cleanly (`mvn clean test`).
5. Address code review feedback promptly.

---

## 💬 Community & Questions

* **Discussions & Q&A**: [GitHub Discussions](https://github.com/gaurav10610/oxmq/discussions)
* **Issues & Bugs**: [GitHub Issues](https://github.com/gaurav10610/oxmq/issues)
