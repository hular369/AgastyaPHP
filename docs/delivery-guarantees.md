# Delivery Guarantees & Consistency in Agastya PHP

Distributed systems operate across asynchronous network boundaries. To build reliable systems, engineering teams must evaluate delivery guarantees with absolute clarity and precision.

---

## 1. At-Least-Once Delivery Semantics

Agastya operates under an **at-least-once** delivery guarantee.

```
[Producer]  ---(Publish: acks=-1)--->  [Apache Kafka]  ---(poll)--->  [Consumer Worker]
                                                                              |
                                                                        (Process & Handle)
                                                                              v
                                                                        [Explicit Commit]
```

### Why "Exactly-Once" is a Myth Across External Side-Effects
Apache Kafka supports idempotent producers (`enable.idempotence=true`) and transactional multi-topic writes. However, consumer business logic inevitably produces side effects outside Kafka:
- Charging a credit card via Stripe
- Executing an SQL `INSERT` or `UPDATE`
- Sending a transactional notification email

Because distributed two-phase commits (2PC) across Kafka and third-party APIs are either impossible or prohibitively expensive, an unexpected crash or network partition during offset commit will cause Kafka to re-deliver previously processed records.

### Scenarios Causing Re-Delivery:
1. **Worker Crash After Execution**: Worker executes business logic (e.g. database update), but crashes before sending the offset commit to Kafka. The rebalance assigns the partition to a new worker, which re-consumes from the last committed offset.
2. **Consumer Group Rebalance**: When a node joins, leaves, or exceeds its session timeout, Kafka triggers a rebalance. In-flight messages without committed offsets are reassigned and re-processed.
3. **Transient Network Partitions**: Broker network drops cause commit requests to time out, prompting the client to reconnect and resume from the previous acknowledged position.

---

## 2. Idempotent Consumer Design

Because duplicate messages are guaranteed to occur in distributed architectures, consumers **must be designed to be idempotent**:

```
                       +-------------------------+
                       |    Incoming Record      |
                       +-------------------------+
                                    |
                                    v
                     +-----------------------------+
                     | Is event.id in Dedup Store? |
                     +-----------------------------+
                               /         \
                       [Yes]  /           \  [No]
                             v             v
        +-------------------------+   +-------------------------+
        | Skip Handler Execution  |   | Record event.id + TTL   |
        | Log Duplicate Metric    |   | Execute Business Logic  |
        | Acknowledge Offset      |   | Acknowledge Offset      |
        +-------------------------+   +-------------------------+
```

### Idempotency in Agastya:
1. **CloudEvents Unique UUID**: Every event generated via `Event::create()` assigns a unique UUIDv4.
2. **Deduplicator Contract**: Agastya checks the event ID against `DeduplicatorInterface` before invoking domain handlers.
3. **Database Constraints**: In production applications, handlers should also leverage database unique keys (e.g. `UNIQUE KEY (order_id)`) to enforce atomic idempotency within application transactions.

---

## 3. Offset Commit Integrity

In Apache Kafka, partition offsets are monotonically increasing 64-bit integers. Committing offset $N$ implicitly marks all offsets $< N$ as acknowledged.

### Agastya's Guarantees:
- **No Gap Commits**: Offsets are committed strictly after handler completion or after durable delivery to the Dead Letter Queue.
- **Poison-Pill Resolution**: Fatal non-retryable errors are routed to `{topic}.dlq` before acknowledging the offset, preventing poison pills from stalling partition progress indefinitely while guaranteeing zero message loss.
- **Graceful Signal Draining**: When `SIGTERM` or `SIGINT` is received, workers finish processing their current record and flush remaining offset commits before the process exits.
