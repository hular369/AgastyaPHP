<?php

declare(strict_types=1);

namespace Agastya\Dlq;

use Psr\Log\LoggerInterface;
use Psr\Log\NullLogger;

final class DlqPublisher implements DlqPublisherInterface
{
    /** @var callable(string $topic, ?string $key, string $payload, array $headers): void */
    private $produceCallback;
    private LoggerInterface $logger;

    /**
     * @param callable(string $topic, ?string $key, string $payload, array $headers): void $produceCallback
     */
    public function __construct(
        callable $produceCallback,
        ?LoggerInterface $logger = null
    ) {
        $this->produceCallback = $produceCallback;
        $this->logger = $logger ?? new NullLogger();
    }

    public function resolveTopic(string $originalTopic): string
    {
        return sprintf('%s.dlq', $originalTopic);
    }

    public function publish(DlqEnvelope $envelope): void
    {
        $dlqTopic = $this->resolveTopic($envelope->originalTopic);
        $payload = $envelope->toJson();
        $headers = $envelope->getHeaders();
        $key = $envelope->originalEvent->key;

        $this->logger->warning('Routing permanently failed event to Dead Letter Queue', [
            'dlq_topic' => $dlqTopic,
            'event_id' => $envelope->originalEvent->id,
            'original_topic' => $envelope->originalTopic,
            'error' => $envelope->errorReason,
            'attempts' => $envelope->failedAttempts,
        ]);

        ($this->produceCallback)($dlqTopic, $key, $payload, $headers);
    }
}
