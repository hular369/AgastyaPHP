# StreamForge PHP Architecture

StreamForge is a production-ready, distributed event processing framework built in modern PHP (8.2+) for Apache Kafka.

It provides a high-reliability operational runtime encapsulating multi-process worker pools, exponential backoff, dead-letter queue (DLQ) routing, idempotency deduplication, and Prometheus observability.

---

## High-Level Topology

```
                  +-----------------------------------------------+
                  |               Event Producers                 |
                  |     (Laravel, Symfony, Microservices, CLI)    |
                  +-----------------------------------------------+
                                          |
                                          v
                          +-------------------------------+
                          |     StreamForge Producer      |
                          |  (Validation + CloudEvents)   |
                          +-------------------------------+
                                          |
                                          v
                  +-----------------------------------------------+
                  |             Apache Kafka Broker               |
                  |            (KRaft Mode, Topics)               |
                  +-----------------------------------------------+
                                          |
                                          v
                  +-----------------------------------------------+
                  |          StreamForge Worker Supervisor        |
                  |         (pcntl_fork Process Manager)          |
                  +-----------------------------------------------+
                                          |
        +---------------------------------+---------------------------------+
        |                                 |                                 |
        v                                 v                                 v
+---------------+                 +---------------+                 +---------------+
| Child Worker  |                 | Child Worker  |                 | Child Worker  |
|  [PID: 101]   |                 |  [PID: 102]   |                 |  [PID: 103]   |
+---------------+                 +---------------+                 +---------------+
        |                                 |                                 |
        +---------------------------------+---------------------------------+
                                          |
                        +-----------------+-----------------+
                        |                                   |
                 [Success Path]                      [Failure Path]
                        |                                   |
                        v                                   v
              +-------------------+               +-------------------+
              | Mark & Commit     |               | Retry Engine      |
              | Kafka Offset      |               | (Exp. Backoff)    |
              +-------------------+               +-------------------+
                                                            |
                                            +---------------+---------------+
                                            |                               |
                                       [Exhausted]                  [Non-Retryable]
                                            |                               |
                                            +---------------+---------------+
                                                            |
                                                            v
                                                  +-------------------+
                                                  | DLQ Publisher     |
                                                  | (orders.v1.dlq)   |
                                                  +-------------------+
                                                            |
                                                            v
                                                  +-------------------+
                                                  | Mark & Commit     |
                                                  | Kafka Offset      |
                                                  +-------------------+
```

---

## Core Components

### 1. CloudEvents Model (`StreamForge\Event\Event`)
Conforms to the [CloudEvents v1.0 standard](https://cloudevents.io/):
- **id**: Universally unique UUIDv4.
- **type**: Domain event discriminator (e.g. `order.created`, `payment.completed`).
- **source**: Producer service identifier.
- **specversion**: `'1.0'`.
- **time**: UTC RFC3339 timestamp.
- **key**: Optional partition key determining Kafka partition routing.
- **data**: Payload array.
- **metadata**: Key-value context headers (correlation IDs, trace contexts).

### 2. Producer (`StreamForge\Producer\Producer`)
- Validates event schema before publishing.
- Injects CloudEvents headers (`ce_id`, `ce_type`, `ce_source`, `ce_time`) as native Kafka headers.
- Transports through `KafkaClientInterface`, supporting pure-PHP `LonglangKafkaClient` or `InMemoryKafkaClient`.
- Records real-time Prometheus publishing counters.

### 3. Worker Supervisor & Concurrency (`StreamForge\Consumer\WorkerSupervisor`)
- Solves PHP's single-threaded nature in CLI daemon mode via `pcntl_fork()`.
- Spawns and supervises $N$ worker processes.
- Monitors child PIDs using non-blocking `pcntl_waitpid()`.
- Automatically respawns crashed workers for self-healing operation.
- Gracefully broadcasts `SIGTERM` to all children upon supervisor shutdown.

### 4. Memory Management in Long-Running Daemons
PHP was historically engineered for ephemeral request-response lifecycles. In long-running CLI worker daemons, memory leaks can cause catastrophic process bloat.
StreamForge mitigates this through:
- Cyclic garbage collection: triggers `gc_collect_cycles()` every 100 processed messages.
- Object graph decoupling: avoids persistent static object retainment.
- Worker process cycling: supervisors can optionally terminate and respawn workers after a configured message threshold.

### 5. Retry Engine (`StreamForge\Retry\RetryEngine`)
- Configurable exponential backoff: `delay = min(maxInterval, initialInterval * (multiplier ^ (attempt - 2)))`.
- Full jitter randomization to avoid thundering-herd spikes against downstream databases.
- Distinguishes between transient glitches and permanent failures (`NonRetryableException`), terminating immediately on non-retryable errors.

### 6. Dead Letter Queue (`StreamForge\Dlq\DlqPublisher`)
- Preserves forensic evidence for permanent failures:
  - Original event payload and metadata.
  - Failure timestamp and attempt count.
  - Broker coordinates: topic, partition, offset.
  - Exception message, stack trace, and originating worker PID/hostname.
- Routes to `{original_topic}.dlq`.
- Acknowledges the offset so the consumer partition advances without stalling.

### 7. Idempotency (`StreamForge\Idempotency\MemoryDeduplicator`)
- Protects consumers against duplicate side-effects caused by Kafka at-least-once re-deliveries.
- In-memory store with automatic TTL expiration sweep.
- Clean `DeduplicatorInterface` contract ready for Redis or SQL drop-in replacement.

### 8. Observability (`StreamForge\Observability\Metrics`)
- Exposes OpenMetrics / Prometheus metrics using `promphp/prometheus_client_php`:
  - `streamforge_events_published_total{topic, status}`
  - `streamforge_events_consumed_total{topic, status}`
  - `streamforge_event_processing_duration_seconds{topic, event_type}`
  - `streamforge_event_retries_total{topic, event_type}`
  - `streamforge_dlq_events_total{topic, reason}`
  - `streamforge_active_workers{consumer_group}`
