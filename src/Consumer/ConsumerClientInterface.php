<?php

declare(strict_types=1);

namespace Agastya\Consumer;

interface ConsumerClientInterface
{
    /**
     * Polls Kafka for a single record. Returns null if timeout expires without messages.
     */
    public function poll(int $timeoutMs = 1000): ?Record;

    /**
     * Acknowledges / commits the offset for the processed record.
     */
    public function ack(Record $record): void;

    /**
     * Closes the consumer and leaves the consumer group.
     */
    public function close(): void;
}
