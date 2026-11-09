# StreamForge PHP

[![CI](https://github.com/raul369/php-event-processing-framework/actions/workflows/ci.yml/badge.svg)](https://github.com/raul369/php-event-processing-framework/actions/workflows/ci.yml)
[![PHP Version](https://img.shields.io/badge/php-%3E%3D%208.2-8892BF.svg)](https://php.net)
[![License](https://img.shields.io/badge/license-MIT-blue.svg)](LICENSE)
[![Apache Kafka](https://img.shields.io/badge/broker-Apache%20Kafka%20(KRaft)-231F20.svg)](https://kafka.apache.org)

> **A PHP-based distributed event processing framework with concurrent workers, configurable retries, dead-letter queues and Prometheus observability.**

StreamForge is an extensible, reliable distributed event processing framework built in modern PHP (8.2+) for **Apache Kafka**. It bridges the gap between raw Kafka protocol clients and production operational concerns—providing concurrent multi-process worker pools, exponential backoff with jitter, dead-letter queue routing, idempotent deduplication, and Prometheus telemetry out-of-the-box.

---

## Architecture Overview

```
                   StreamForge Event Processing Pipeline (PHP)

  [Event Producers]                  [Apache Kafka Broker]                 [StreamForge Supervisor]
+-------------------+              +-----------------------+             +--------------------------+
|  Applications     |              | Topics:               |             | Multi-Process Supervisor |
|  REST APIs        |              |  • orders.v1          |             | (pcntl_fork Manager)     |
|  CLI Publisher    |              |  • payments.v1        |             +--------------------------+
+-------------------+              |  • inventory.v1       |                          |
          |                        +-----------------------+                          v
          v                                    |                         +--------------------------+
+-------------------+                          v                         | Concurrent Child Workers |
| StreamForge       | -----------> [Partition Broker] ----------->       |  • Worker PID 101        |
| Publisher         |                                                    |  • Worker PID 102        |
| (CloudEvents JSON)|                                                    +--------------------------+
+-------------------+                                                                 |
                                                                        +-------------+-------------+
                                                                        |                           |
                                                                        v                           v
                                                                 [Success Path]              [Failure Path]
                                                                        |                           |
                                                              +-------------------+       +-------------------+
                                                              | Acknowledge &     |       | Retry Engine      |
                                                              | Commit Offset     |       | (Exp. Backoff)    |
                                                              +-------------------+       +-------------------+
                                                                                                    |
                                                                                    +---------------+---------------+
                                                                                    |                               |
                                                                               [Exhausted]                  [Non-Retryable]
                                                                                    |                               |
                                                                                    +---------------+---------------+
                                                                                                    |
                                                                                                    v
                                                                                          +-------------------+
                                                                                          | Dead Letter Queue |
                                                                                          | (*.dlq topic)     |
                                                                                          +-------------------+
```

---

## Key Capabilities

- **CloudEvents v1.0 Compliant**: Built-in envelope with UUIDv4 IDs, UTC RFC3339 timestamps, partition routing keys, and extensible metadata headers.
- **Multi-Process Concurrency**: Solves PHP CLI concurrency using `pcntl_fork()` and `posix` process management, with automatic worker respawn on unexpected exits.
- **Memory Safety in Long-Running Daemons**: Proactively manages memory via automated cyclic garbage collection (`gc_collect_cycles()`), preventing memory bloat in 24/7 worker daemons.
- **Resilient Retry Engine**: Exponential backoff with full jitter and non-retryable error detection (e.g. fatal validation errors abort immediately to prevent partition stalls).
- **Forensic Dead Letter Queue (DLQ)**: Preserves forensic failure metadata (original topic, partition, offset, failure timestamp, attempt count, exception message, stack trace, and worker PID/hostname).
- **Idempotency & Deduplication**: Built-in TTL deduplicator preventing duplicate side-effects from Kafka at-least-once re-deliveries.
- **Native Prometheus Observability**: Built-in OpenMetrics collectors tracking throughput, handler execution duration histograms, retry frequencies, DLQ routing, and active worker gauges.
- **Pure PHP Kafka Client**: Operates over TCP stream sockets with zero external C-extension (`librdkafka`) requirements, enabling universal cross-platform deployment.
- **Graceful Shutdown**: Catches OS signals (`SIGTERM`, `SIGINT`), cleanly drains in-flight jobs, flushes remaining offset commits, and shuts down child processes safely.

---

## Technology Stack

| Component | Technology | Rationale |
|---|---|---|
| **Language** | PHP 8.2+ | Strict types (`declare(strict_types=1);`), readonly classes, match expressions |
| **Event Broker** | Apache Kafka (KRaft Mode) | High-throughput distributed log storage without legacy ZooKeeper dependencies |
| **Kafka Client** | `longlang/phpkafka` | 100% pure PHP socket client, zero C-extension (`librdkafka`) dependency |
| **Concurrency** | `pcntl_fork` & `posix` | True process isolation, copy-on-write memory efficiency, self-healing supervisor |
| **Metrics** | Prometheus | Standard OpenMetrics exposition (`promphp/prometheus_client_php`) |
| **Local Dev** | Docker Compose | Local Apache Kafka (KRaft mode), Kafka UI, and Prometheus |
| **Testing** | Zero-dependency Test Runner | Instant test execution with colored reporting without missing XML extension issues |

---

## Quickstart

### 1. Prerequisites
- PHP 8.2 or later (`php -v`)
- Composer (`composer -V`)
- Docker & Docker Compose (optional for live broker)

### 2. Run the Test Suite
```bash
make test
```
Output:
```
StreamForge PHP Test Runner
============================================================
ConsumerTest
  ✔ testProcessRecordSuccess (3.56ms)
  ✔ testProcessRecordCorruptJsonRoutesToDlq (0.11ms)
  ✔ testProcessRecordDeduplication (0.1ms)
  ...
OK (25 tests, 25 passed) in 1007ms (Peak Memory: 4MB)
```

### 3. Run the E-Commerce Pipeline Simulation
You can immediately experience the entire StreamForge pipeline **in-memory** without running Docker:

```bash
make demo
```
Output:
```
================================================================================
           STREAMFORGE PHP DISTRIBUTED EVENT PROCESSING PIPELINE DEMO           
================================================================================
Mode: SIMULATED (Demonstrating retries, DLQ, idempotency, and offset management)

--- SCENARIO 1: Happy Path Processing ---
   ✅ [ORDER HANDLER] Processed Order ord-1001 for customer cust-42 ($199.95)
   ✅ [PAYMENT HANDLER] Confirmed Payment pay-9001 for Order ord-1001 (SETTLED)

--- SCENARIO 2: Transient Failure with Exponential Retry ---
   ⚠️  [INVENTORY HANDLER] Simulated database connection timeout on attempt 1...
      [retry-sleep] backing off for 50ms...
   ⚠️  [INVENTORY HANDLER] Simulated database connection timeout on attempt 2...
      [retry-sleep] backing off for 100ms...
   ✅ [INVENTORY HANDLER] Successfully reserved SKU LAPTOP-PRO-16 (Qty: 5) after 3 attempts!

--- SCENARIO 3: Poison Pill & Dead Letter Queue (DLQ) Routing ---
   🚨 [DLQ ROUTED] Event a43b10e0 routed to DLQ!
      Reason: Poison pill: invalid negative order amount -999.00 (Failed Attempts: 1)

--- SCENARIO 4: Idempotent Deduplication (At-Least-Once Re-Delivery) ---
   Re-delivering identical event 5b70146f to simulate Kafka network retry...

================================================================================
                            SUMMARY & VERIFICATION                              
================================================================================
• Total offsets marked for commit: 5
• DLQ envelopes captured: 1
• Idempotency cache active items: 4
All scenarios processed successfully with zero data loss!
```

---

### 4. Running with Live Apache Kafka Infrastructure

#### Step A: Start Apache Kafka (KRaft), Kafka UI & Prometheus
```bash
make docker-up
```
- Apache Kafka broker: `localhost:9092`
- Kafka UI: `http://localhost:8085`
- Prometheus UI: `http://localhost:9090`

#### Step B: Start the Worker Daemon
```bash
make worker
```
Spawns 4 concurrent worker processes supervised via `pcntl_fork`, subscribing to `orders.v1,payments.v1,inventory.v1`.

#### Step C: Publish an Event via CLI
```bash
php bin/publish orders.v1 order.created '{"order_id":"ord-101","amount":150.00,"customer_id":"cust-88"}' ord-101
```

---

## Developer Guide: Code Walkthrough

### 1. Defining and Registering Event Handlers
```php
use StreamForge\Event\Event;
use StreamForge\Exception\NonRetryableException;
use StreamForge\StreamForge;

$engine = StreamForge::create([
    'brokers' => ['127.0.0.1:9092'],
    'group_id' => 'order-processors',
    'topics' => ['orders.v1'],
    'workers' => 4,
    'retry' => [
        'max_attempts' => 3,
        'initial_interval_ms' => 100,
        'max_interval_ms' => 2000,
    ],
    'dlq' => true,
    'deduplication' => true,
]);

// Register event handler with closure or HandlerInterface
$engine->registerCallable('order.created', function (Event $event): void {
    $amount = (float) ($event->data['amount'] ?? 0);

    if ($amount <= 0) {
        // Fatal validation failure: bypass retries and route directly to DLQ
        throw new NonRetryableException("Invalid order amount {$amount}");
    }

    // Process order...
});

$engine->run();
```

### 2. Publishing Events
```php
use StreamForge\Event\Event;
use StreamForge\StreamForge;

$producer = StreamForge::createProducer([
    'brokers' => ['127.0.0.1:9092'],
]);

$event = Event::create(
    type: 'order.created',
    source: 'checkout-service',
    data: ['order_id' => 'ord-555', 'amount' => 89.90],
    key: 'ord-555',
    metadata: ['tenant_id' => 'us-east']
);

$producer->publish('orders.v1', $event);
$producer->close();
```

---

## Observability & Prometheus Metrics

| Metric Name | Type | Labels | Description |
|---|---|---|---|
| `streamforge_events_published_total` | Counter | `topic`, `status` | Total count of published events |
| `streamforge_events_consumed_total` | Counter | `topic`, `status` | Total count of consumed events by status (`success`, `corrupt`, `duplicate`, `unhandled`, `dlq`) |
| `streamforge_event_processing_duration_seconds` | Histogram | `topic`, `event_type` | Handler latency histogram |
| `streamforge_event_retries_total` | Counter | `topic`, `event_type` | Count of transient error retry attempts |
| `streamforge_dlq_events_total` | Counter | `topic`, `reason` | Count of events routed to Dead Letter Queues |
| `streamforge_active_workers` | Gauge | `consumer_group` | Current count of active worker processes |

---

## Architectural Deep Dives

For detailed engineering discussions and trade-off analyses:
- [System Architecture](file:///home/raul/php-event-processing-framework/docs/architecture.md): Concurrency topology, process supervisor, and memory management.
- [Delivery Guarantees & Consistency](file:///home/raul/php-event-processing-framework/docs/delivery-guarantees.md): Honest evaluation of at-least-once delivery, rebalance behavior, and idempotency.
- [Engineering Trade-Offs](file:///home/raul/php-event-processing-framework/docs/trade-offs.md): Multi-process vs event loops, pure PHP vs C-extensions, and Apache Kafka KRaft mode.

---

## Project Structure

```
.
├── bin/
│   ├── demo                  # Runnable end-to-end e-commerce simulation
│   ├── publish               # CLI utility to publish test events to Kafka
│   ├── test                  # Fast, zero-dependency test runner
│   └── worker                # Production multi-process worker daemon
├── deployments/
│   ├── docker-compose.yml    # Apache Kafka (KRaft mode), Kafka UI & Prometheus
│   └── prometheus.yml        # Prometheus scraping configuration
├── docs/
│   ├── architecture.md       # Component breakdown & long-running daemon design
│   ├── delivery-guarantees.md # At-least-once delivery, rebalance, & idempotency
│   └── trade-offs.md         # Technical trade-offs in PHP event processing
├── src/
│   ├── Consumer/             # Consumer loop, HandlerRegistry, multi-process WorkerSupervisor
│   ├── Dlq/                  # Dead Letter Queue publisher & forensic DlqEnvelope
│   ├── Event/                # CloudEvents v1.0 schema envelope, validation & JSON serialization
│   ├── Exception/            # Custom exception hierarchy (NonRetryable, RetriesExhausted)
│   ├── Idempotency/          # DeduplicatorInterface & MemoryDeduplicator with TTL cleanup
│   ├── Observability/        # Prometheus metrics collectors & exposition renderer
│   ├── Producer/             # Pure-PHP Kafka publisher with CloudEvents header injection
│   ├── Retry/                # Exponential backoff engine with full jitter
│   └── StreamForge.php       # Central public framework facade & factory
├── tests/
│   └── Unit/                 # Comprehensive unit test suite
├── .github/
│   └── workflows/ci.yml      # Automated GitHub Actions CI pipeline
├── composer.json             # PSR-4 autoloading and dependencies
├── Makefile                  # Developer workflow automation
└── README.md
```

---

## License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.
