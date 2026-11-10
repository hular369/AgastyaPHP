<?php

declare(strict_types=1);

namespace Agastya\Dlq;

use DateTimeImmutable;
use DateTimeInterface;
use JsonSerializable;
use Agastya\Event\Event;

final readonly class DlqEnvelope implements JsonSerializable
{
    public function __construct(
        public Event $originalEvent,
        public string $originalTopic,
        public int $originalPartition,
        public int $originalOffset,
        public string $errorReason,
        public int $failedAttempts,
        public DateTimeImmutable $failedAt,
        public int $workerPid,
        public string $workerHostname,
        public array $metadata = []
    ) {
    }

    public static function create(
        Event $event,
        string $topic,
        int $partition,
        int $offset,
        string $errorReason,
        int $attempts = 1,
        array $metadata = []
    ): self {
        return new self(
            originalEvent: $event,
            originalTopic: $topic,
            originalPartition: $partition,
            originalOffset: $offset,
            errorReason: $errorReason,
            failedAttempts: $attempts,
            failedAt: new DateTimeImmutable('now', new \DateTimeZone('UTC')),
            workerPid: getmypid() ?: 0,
            workerHostname: gethostname() ?: 'unknown-host',
            metadata: $metadata
        );
    }

    public function getHeaders(): array
    {
        return [
            'x-dlq-original-topic' => $this->originalTopic,
            'x-dlq-original-partition' => (string) $this->originalPartition,
            'x-dlq-original-offset' => (string) $this->originalOffset,
            'x-dlq-error' => $this->errorReason,
            'x-dlq-failed-attempts' => (string) $this->failedAttempts,
            'x-dlq-failed-at' => $this->failedAt->format(DateTimeInterface::RFC3339_EXTENDED),
            'x-dlq-worker-pid' => (string) $this->workerPid,
            'x-dlq-worker-host' => $this->workerHostname,
        ];
    }

    public function toArray(): array
    {
        return [
            'original_event' => $this->originalEvent->toArray(),
            'original_topic' => $this->originalTopic,
            'original_partition' => $this->originalPartition,
            'original_offset' => $this->originalOffset,
            'error_reason' => $this->errorReason,
            'failed_attempts' => $this->failedAttempts,
            'failed_at' => $this->failedAt->format(DateTimeInterface::RFC3339_EXTENDED),
            'worker_pid' => $this->workerPid,
            'worker_hostname' => $this->workerHostname,
            'metadata' => $this->metadata,
        ];
    }

    public function toJson(): string
    {
        return json_encode($this->toArray(), JSON_UNESCAPED_SLASHES | JSON_THROW_ON_ERROR);
    }

    public function jsonSerialize(): array
    {
        return $this->toArray();
    }
}
