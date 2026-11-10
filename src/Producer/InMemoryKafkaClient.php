<?php

declare(strict_types=1);

namespace Agastya\Producer;

final class InMemoryKafkaClient implements KafkaClientInterface
{
    /** @var array<string, list<array{key: ?string, value: string, headers: array<string, string>, partition: int, offset: int}>> */
    private array $records = [];
    private bool $closed = false;

    public function send(string $topic, ?string $key, string $value, array $headers = []): void
    {
        if ($this->closed) {
            throw new \RuntimeException('InMemoryKafkaClient is closed.');
        }

        if (!isset($this->records[$topic])) {
            $this->records[$topic] = [];
        }

        $offset = count($this->records[$topic]);
        $partition = 0; // Default partition

        $this->records[$topic][] = [
            'key' => $key,
            'value' => $value,
            'headers' => $headers,
            'partition' => $partition,
            'offset' => $offset,
        ];
    }

    public function close(): void
    {
        $this->closed = true;
    }

    /**
     * @return array<string, list<array{key: ?string, value: string, headers: array<string, string>, partition: int, offset: int}>>
     */
    public function getRecords(): array
    {
        return $this->records;
    }

    /**
     * @return list<array{key: ?string, value: string, headers: array<string, string>, partition: int, offset: int}>
     */
    public function getTopicRecords(string $topic): array
    {
        return $this->records[$topic] ?? [];
    }

    public function count(string $topic): int
    {
        return count($this->records[$topic] ?? []);
    }

    public function isClosed(): bool
    {
        return $this->closed;
    }
}
