<?php

declare(strict_types=1);

namespace Agastya\Consumer;

final class InMemoryConsumerClient implements ConsumerClientInterface
{
    /** @var list<Record> */
    private array $queue = [];
    /** @var list<Record> */
    private array $acked = [];
    private bool $closed = false;

    /**
     * @param list<Record> $records
     */
    public function __construct(array $records = [])
    {
        $this->queue = $records;
    }

    public function enqueue(Record $record): void
    {
        $this->queue[] = $record;
    }

    public function poll(int $timeoutMs = 1000): ?Record
    {
        if ($this->closed || empty($this->queue)) {
            return null;
        }

        return array_shift($this->queue);
    }

    public function ack(Record $record): void
    {
        $this->acked[] = $record;
    }

    public function close(): void
    {
        $this->closed = true;
    }

    /**
     * @return list<Record>
     */
    public function getAcked(): array
    {
        return $this->acked;
    }

    public function countQueue(): int
    {
        return count($this->queue);
    }

    public function countAcked(): int
    {
        return count($this->acked);
    }

    public function isClosed(): bool
    {
        return $this->closed;
    }
}
