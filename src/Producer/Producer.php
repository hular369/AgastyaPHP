<?php

declare(strict_types=1);

namespace StreamForge\Producer;

use DateTimeInterface;
use Psr\Log\LoggerInterface;
use Psr\Log\NullLogger;
use StreamForge\Event\Event;
use StreamForge\Observability\Metrics;
use Throwable;

final class Producer implements ProducerInterface
{
    private LoggerInterface $logger;

    public function __construct(
        private readonly KafkaClientInterface $client,
        private readonly ?Metrics $metrics = null,
        ?LoggerInterface $logger = null
    ) {
        $this->logger = $logger ?? new NullLogger();
    }

    public function publish(string $topic, Event $event): void
    {
        $event->validate();
        $payload = $event->toJson();

        $headers = [
            'ce_id' => $event->id,
            'ce_type' => $event->type,
            'ce_source' => $event->source,
            'ce_specversion' => $event->specversion,
            'ce_time' => $event->time->format(DateTimeInterface::RFC3339_EXTENDED),
        ];

        foreach ($event->metadata as $k => $v) {
            $headers[$k] = (string) $v;
        }

        try {
            $this->client->send($topic, $event->key, $payload, $headers);
            $this->metrics?->recordPublished($topic, 'success');

            $this->logger->info('Published event to Kafka topic', [
                'topic' => $topic,
                'event_id' => $event->id,
                'event_type' => $event->type,
                'key' => $event->key,
            ]);
        } catch (Throwable $e) {
            $this->metrics?->recordPublished($topic, 'error');

            $this->logger->error('Failed to publish event to Kafka topic', [
                'topic' => $topic,
                'event_id' => $event->id,
                'event_type' => $event->type,
                'error' => $e->getMessage(),
            ]);

            throw $e;
        }
    }

    public function close(): void
    {
        $this->client->close();
    }
}
