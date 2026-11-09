# Engineering Trade-Offs in StreamForge PHP

Building distributed systems in PHP requires deliberate architectural decisions, especially when managing long-running processes, socket I/O, and consumer concurrency.

---

## 1. Concurrency: Multi-Process (`pcntl_fork`) vs. Async Event Loops vs. Goroutines

| Dimension | Multi-Process (`pcntl_fork`) (StreamForge Choice) | Async Event Loops (Swoole / ReactPHP / Fibers) | Go Goroutines / OS Threads |
|---|---|---|---|
| **Memory Isolation** | **Complete**. Each child process has its own address space. A memory leak or fatal error in worker 1 does not crash worker 2. | Shared memory. A fatal crash or uncaught exception terminates the entire event loop. | Shared memory with lightweight scheduling (2KB stack). |
| **Compatibility** | Standard PHP CLI with `ext-pcntl` and `ext-posix`. Zero third-party C-extensions required. | Requires custom PHP extensions (Swoole) or non-blocking driver rewrites (ReactPHP). | Native runtime feature of Go. |
| **I/O Model** | Blocking I/O per worker process. Highly suitable for synchronous DB calls (PDO, Doctrine). | Non-blocking asynchronous I/O. Requires non-blocking DB drivers. | Non-blocking cooperative netpoller. |
| **Memory Overhead** | Higher (Copy-On-Write mitigates this, ~15-25MB per process). | Lower (~10-20MB total). | Negligible (~few KB per worker). |

### Our Decision:
We chose the **Multi-Process Worker Supervisor (`pcntl_fork`)** model. It provides bulletproof process isolation, works seamlessly with standard synchronous PHP libraries (PDO, Doctrine, cURL, Redis), and includes automatic process respawning if a worker encounters a fatal error.

---

## 2. Kafka Client: Pure PHP (`longlang/phpkafka`) vs. CGO Extension (`ext-rdkafka`)

| Metric | Pure PHP (`longlang/phpkafka`) | C Extension (`ext-rdkafka` / `librdkafka`) |
|---|---|---|
| **Installation** | `composer require longlang/phpkafka`. Runs on standard PHP everywhere. | Requires compiling `librdkafka` C library and enabling `extension=rdkafka.so`. |
| **Portability** | Universal (Docker, Kubernetes, local machines, Alpine, macOS). | Fragile across container base images and PHP minor upgrades. |
| **Debugging** | 100% PHP stack traces, visible in Xdebug. | Core dumps and C-level memory faults harder to trace in PHP. |
| **Raw Throughput** | Moderate-to-High (10,000 - 30,000 msg/sec per node). | Extremely High (100,000+ msg/sec via C buffer). |

### Our Decision:
We selected a pure PHP architecture with an adapter interface (`KafkaClientInterface` and `ConsumerClientInterface`). This guarantees that any developer or CI pipeline can run the framework out-of-the-box without fighting C-library compile errors, while allowing high-throughput enterprise deployments to swap in an `ext-rdkafka` adapter if desired.

---

## 3. Broker: Apache Kafka (KRaft Mode) vs. Redpanda

| Attribute | Apache Kafka (KRaft Mode) (Current) | Redpanda |
|---|---|---|
| **Ecosystem Standard** | Official Apache open-source streaming platform. De-facto industry standard. | Kafka-compatible C++ re-implementation. |
| **Architecture** | Native Raft consensus metadata protocol (no ZooKeeper required since Kafka 3.3+). | Built-in Raft, zero JVM overhead. |
| **Production Dominance** | Vast majority of enterprise installations (AWS MSK, Confluent, bare metal). | Growing developer and edge adoption. |
| **Operational Parity** | Standard Kafka tools (`kafka-topics.sh`, Kafka UI, JMX monitoring). | Compatible with Kafka protocol tools and `rpk`. |

### Our Decision:
For this project, we explicitly standardized on **Apache Kafka in KRaft mode**. KRaft removes the legacy ZooKeeper operational burden, allowing a single lightweight container in Docker Compose to provide full native Apache Kafka streaming.

---

## 4. Failure Handling: In-Process Exponential Retries vs. Delayed Retry Topics

| Dimension | In-Process Retries (Current MVP) | Delayed Retry Topics (Enterprise) |
|---|---|---|
| **Broker Complexity** | None. Zero additional topics or schedulers. | High. Requires separate `$topic.retry.10s`, `$topic.retry.1m`, `$topic.retry.10m` topics. |
| **Partition Stalling** | Pauses worker process during sleep backoff. | Non-blocking. Failed message is published to retry topic and immediately acked on main topic. |
| **Recovery Latency** | Immediate self-healing for transient flakes (50ms - 2000ms). | Delayed by scheduled poll intervals. |
| **Optimal Use Case** | Ephemeral DB deadlocks, brief socket timeouts. | Prolonged downstream outages (e.g. 30-minute third-party payment outage). |

### Our Decision:
We implemented **bounded exponential backoff with full jitter and non-retryable error detection**. Bounding the maximum sleep interval and immediately routing non-retryable errors to the DLQ prevents partition stalls while providing immediate self-healing for transient errors.
