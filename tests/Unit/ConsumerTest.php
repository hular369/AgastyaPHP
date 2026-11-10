<?php

declare(strict_types=1);

namespace Agastya\Tests\Unit;

use Exception;
use Agastya\Consumer\Consumer;
use Agastya\Consumer\HandlerRegistry;
use Agastya\Consumer\InMemoryConsumerClient;
use Agastya\Consumer\Record;
use Agastya\Dlq\DlqEnvelope;
use Agastya\Dlq\DlqPublisherInterface;
use Agastya\Event\Event;
use Agastya\Exception\NonRetryableException;
use Agastya\Idempotency\MemoryDeduplicator;
use Agastya\Observability\Metrics;
use Agastya\Retry\RetryPolicy;

final class ConsumerTest
{
    public function testProcessRecordSuccess(): void
    {
        $client = new InMemoryConsumerClient();
        $registry = new HandlerRegistry();
        $metrics = new Metrics();

        $handled = false;
        $registry->registerCallable('order.created', function (Event $event) use (&$handled): void {
            $handled = true;
            assert($event->type === 'order.created');
            assert($event->data['order_id'] === 'ord-1');
        });

        $consumer = new Consumer($client, $registry, metrics: $metrics);

        $event = Event::create('order.created', 'orders', ['order_id' => 'ord-1']);
        $record = new Record('orders.v1', 0, 10, $event->toJson(), $event->key);

        $result = $consumer->processRecord($record);
        assert($result === true);
        assert($handled === true);
        assert($client->countAcked() === 1);
    }

    public function testProcessRecordCorruptJsonRoutesToDlq(): void
    {
        $client = new InMemoryConsumerClient();
        $registry = new HandlerRegistry();

        $dlqEnvelopes = [];
        $dlqPublisher = new class ($dlqEnvelopes) implements DlqPublisherInterface {
            private $store;
            public function __construct(&$store)
            {
                $this->store = &$store;
            }
            public function publish(DlqEnvelope $envelope): void
            {
                $this->store[] = $envelope;
            }
        };

        $consumer = new Consumer($client, $registry, dlqPublisher: $dlqPublisher);
        $record = new Record('orders.v1', 0, 5, 'invalid-non-json{{{');

        $result = $consumer->processRecord($record);
        assert($result === false);
        assert(count($dlqEnvelopes) === 1);
        assert(str_contains($dlqEnvelopes[0]->errorReason, 'Malformed JSON'));
        assert($client->countAcked() === 1);
    }

    public function testProcessRecordDeduplication(): void
    {
        $client = new InMemoryConsumerClient();
        $registry = new HandlerRegistry();
        $dedup = new MemoryDeduplicator();

        $handleCount = 0;
        $registry->registerCallable('user.login', function () use (&$handleCount): void {
            $handleCount++;
        });

        $consumer = new Consumer($client, $registry, deduplicator: $dedup);

        $event = Event::create('user.login', 'auth', ['user_id' => 'u-1']);
        $record = new Record('auth.events', 0, 1, $event->toJson());

        // First pass
        $consumer->processRecord($record);
        assert($handleCount === 1);

        // Second pass (same UUID)
        $consumer->processRecord($record);
        assert($handleCount === 1, 'Duplicate event must be skipped');
        assert($client->countAcked() === 2);
    }

    public function testProcessRecordRetryAndSuccess(): void
    {
        $client = new InMemoryConsumerClient();
        $registry = new HandlerRegistry();

        $attempts = 0;
        $registry->registerCallable('payment.process', function () use (&$attempts): void {
            $attempts++;
            if ($attempts < 3) {
                throw new Exception('Transient timeout');
            }
        });

        $consumer = new Consumer(
            client: $client,
            registry: $registry,
            retryPolicy: new RetryPolicy(maxAttempts: 4, initialIntervalMs: 1),
            sleeper: static function (): void {}
        );

        $event = Event::create('payment.process', 'pay', ['amount' => 50]);
        $record = new Record('payments', 0, 1, $event->toJson());

        $result = $consumer->processRecord($record);
        assert($result === true);
        assert($attempts === 3);
        assert($client->countAcked() === 1);
    }

    public function testProcessRecordRetriesExhaustedRoutesToDlq(): void
    {
        $client = new InMemoryConsumerClient();
        $registry = new HandlerRegistry();

        $registry->registerCallable('db.sync', function (): void {
            throw new Exception('Database unreachable');
        });

        $dlqEnvelopes = [];
        $dlqPublisher = new class ($dlqEnvelopes) implements DlqPublisherInterface {
            private $store;
            public function __construct(&$store)
            {
                $this->store = &$store;
            }
            public function publish(DlqEnvelope $envelope): void
            {
                $this->store[] = $envelope;
            }
        };

        $consumer = new Consumer(
            client: $client,
            registry: $registry,
            retryPolicy: new RetryPolicy(maxAttempts: 2, initialIntervalMs: 1),
            dlqPublisher: $dlqPublisher,
            sleeper: static function (): void {}
        );

        $event = Event::create('db.sync', 'sync', ['id' => 999]);
        $record = new Record('sync.v1', 2, 77, $event->toJson());

        $result = $consumer->processRecord($record);
        assert($result === false);
        assert(count($dlqEnvelopes) === 1);
        assert(str_contains($dlqEnvelopes[0]->errorReason, 'Retries exhausted'));
        assert($dlqEnvelopes[0]->originalTopic === 'sync.v1');
        assert($dlqEnvelopes[0]->originalPartition === 2);
        assert($dlqEnvelopes[0]->originalOffset === 77);
        assert($client->countAcked() === 1);
    }

    public function testProcessRecordNonRetryableRoutesToDlq(): void
    {
        $client = new InMemoryConsumerClient();
        $registry = new HandlerRegistry();

        $calls = 0;
        $registry->registerCallable('order.validate', function () use (&$calls): void {
            $calls++;
            throw new NonRetryableException('Negative order amount');
        });

        $dlqEnvelopes = [];
        $dlqPublisher = new class ($dlqEnvelopes) implements DlqPublisherInterface {
            private $store;
            public function __construct(&$store)
            {
                $this->store = &$store;
            }
            public function publish(DlqEnvelope $envelope): void
            {
                $this->store[] = $envelope;
            }
        };

        $consumer = new Consumer(
            client: $client,
            registry: $registry,
            retryPolicy: new RetryPolicy(maxAttempts: 5, initialIntervalMs: 1),
            dlqPublisher: $dlqPublisher,
            sleeper: static function (): void {
                throw new Exception('Sleeper should not be called');
            }
        );

        $event = Event::create('order.validate', 'orders', ['amount' => -100]);
        $record = new Record('orders.v1', 0, 1, $event->toJson());

        $consumer->processRecord($record);
        assert($calls === 1, 'Non-retryable error should only be executed once');
        assert(count($dlqEnvelopes) === 1);
        assert($dlqEnvelopes[0]->errorReason === 'Negative order amount');
        assert($client->countAcked() === 1);
    }
}
