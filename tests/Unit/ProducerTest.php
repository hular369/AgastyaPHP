<?php

declare(strict_types=1);

namespace Agastya\Tests\Unit;

use Agastya\Event\Event;
use Agastya\Observability\Metrics;
use Agastya\Producer\InMemoryKafkaClient;
use Agastya\Producer\Producer;

final class ProducerTest
{
    public function testPublishSuccess(): void
    {
        $client = new InMemoryKafkaClient();
        $metrics = new Metrics();
        $producer = new Producer($client, $metrics);

        $event = Event::create(
            type: 'user.signup',
            source: 'auth-svc',
            data: ['email' => 'user@example.com'],
            key: 'usr-42',
            metadata: ['tenant_id' => 't-1']
        );

        $producer->publish('auth.events', $event);

        assert($client->count('auth.events') === 1);
        $records = $client->getTopicRecords('auth.events');
        $record = $records[0];

        assert($record['key'] === 'usr-42');
        assert($record['headers']['ce_id'] === $event->id);
        assert($record['headers']['ce_type'] === 'user.signup');
        assert($record['headers']['ce_source'] === 'auth-svc');
        assert($record['headers']['tenant_id'] === 't-1');

        $producer->close();
        assert($client->isClosed() === true);
    }
}
