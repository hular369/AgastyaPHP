<?php

declare(strict_types=1);

namespace StreamForge\Tests\Unit;

use StreamForge\Dlq\DlqEnvelope;
use StreamForge\Dlq\DlqPublisher;
use StreamForge\Event\Event;

final class DlqTest
{
    public function testEnvelopeCreationAndHeaders(): void
    {
        $event = Event::create('order.failed', 'orders-svc', ['order_id' => 'ord-99']);
        $envelope = DlqEnvelope::create(
            event: $event,
            topic: 'orders.v1',
            partition: 1,
            offset: 42,
            errorReason: 'Payment gateway timeout',
            attempts: 3
        );

        assert($envelope->originalTopic === 'orders.v1');
        assert($envelope->originalPartition === 1);
        assert($envelope->originalOffset === 42);
        assert($envelope->errorReason === 'Payment gateway timeout');
        assert($envelope->failedAttempts === 3);
        assert($envelope->workerPid > 0);
        assert(!empty($envelope->workerHostname));

        $headers = $envelope->getHeaders();
        assert($headers['x-dlq-original-topic'] === 'orders.v1');
        assert($headers['x-dlq-original-partition'] === '1');
        assert($headers['x-dlq-original-offset'] === '42');
        assert($headers['x-dlq-error'] === 'Payment gateway timeout');
        assert($headers['x-dlq-failed-attempts'] === '3');

        $json = $envelope->toJson();
        $decoded = json_decode($json, true);
        assert($decoded['original_topic'] === 'orders.v1');
        assert($decoded['error_reason'] === 'Payment gateway timeout');
    }

    public function testDlqPublisherRoutesToDlqTopic(): void
    {
        $published = [];
        $publisher = new DlqPublisher(function (string $topic, ?string $key, string $payload, array $headers) use (&$published): void {
            $published[] = [
                'topic' => $topic,
                'key' => $key,
                'payload' => $payload,
                'headers' => $headers,
            ];
        });

        $event = Event::create('test.event', 'test-svc', ['foo' => 'bar'], key: 'entity-123');
        $envelope = DlqEnvelope::create(
            event: $event,
            topic: 'payments.v1',
            partition: 0,
            offset: 10,
            errorReason: 'Card expired',
            attempts: 1
        );

        $publisher->publish($envelope);

        assert(count($published) === 1);
        assert($published[0]['topic'] === 'payments.v1.dlq');
        assert($published[0]['key'] === 'entity-123');
        assert(str_contains($published[0]['payload'], 'Card expired'));
        assert($published[0]['headers']['x-dlq-error'] === 'Card expired');
    }
}
