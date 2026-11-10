<?php

declare(strict_types=1);

namespace Agastya\Tests\Unit;

use DateTimeImmutable;
use Agastya\Event\Event;
use Agastya\Exception\ValidationException;

final class EventTest
{
    public function testCreateValidEvent(): void
    {
        $time = new DateTimeImmutable('2026-11-09T12:00:00Z');
        $event = Event::create(
            type: 'order.created',
            source: 'checkout-service',
            data: ['order_id' => 'ord-123', 'amount' => 99.99],
            key: 'ord-123',
            metadata: ['trace_id' => 'tr-abc'],
            time: $time
        );

        assert(!empty($event->id));
        assert($event->type === 'order.created');
        assert($event->source === 'checkout-service');
        assert($event->key === 'ord-123');
        assert($event->specversion === '1.0');
        assert($event->datacontenttype === 'application/json');
        assert($event->data['order_id'] === 'ord-123');
        assert($event->metadata['trace_id'] === 'tr-abc');
        assert($event->time->getTimestamp() === $time->getTimestamp());
    }

    public function testJsonSerializationRoundtrip(): void
    {
        $event = Event::create(
            type: 'payment.completed',
            source: 'billing',
            data: ['payment_id' => 'pay-555', 'status' => 'success']
        );

        $json = $event->toJson();
        assert(is_string($json));

        $restored = Event::fromJson($json);
        assert($restored->id === $event->id);
        assert($restored->type === $event->type);
        assert($restored->source === $event->source);
        assert($restored->data === $event->data);
    }

    public function testValidationThrowsOnEmptyFields(): void
    {
        $threw = false;
        try {
            new Event(
                id: '',
                type: 'some.event',
                source: 'test',
                data: [],
                time: new DateTimeImmutable()
            );
        } catch (ValidationException) {
            $threw = true;
        }
        assert($threw, 'Should throw ValidationException when id is empty');

        $threw = false;
        try {
            new Event(
                id: 'uuid-1',
                type: '',
                source: 'test',
                data: [],
                time: new DateTimeImmutable()
            );
        } catch (ValidationException) {
            $threw = true;
        }
        assert($threw, 'Should throw ValidationException when type is empty');
    }

    public function testWithMetadata(): void
    {
        $event = Event::create('test.type', 'src', ['foo' => 'bar']);
        $withMeta = $event->withMetadata('correlation_id', 'corr-999');

        assert(!isset($event->metadata['correlation_id']));
        assert($withMeta->metadata['correlation_id'] === 'corr-999');
    }
}
