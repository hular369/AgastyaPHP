<?php

declare(strict_types=1);

namespace StreamForge\Consumer;

use Psr\Log\LoggerInterface;
use Psr\Log\NullLogger;
use StreamForge\Dlq\DlqEnvelope;
use StreamForge\Dlq\DlqPublisherInterface;
use StreamForge\Event\Event;
use StreamForge\Exception\NonRetryableException;
use StreamForge\Exception\RetriesExhaustedException;
use StreamForge\Exception\ValidationException;
use StreamForge\Idempotency\DeduplicatorInterface;
use StreamForge\Observability\Metrics;
use StreamForge\Retry\RetryEngine;
use StreamForge\Retry\RetryPolicy;
use Throwable;

final class Consumer
{
    private LoggerInterface $logger;
    private RetryEngine $retryEngine;
    private bool $running = false;
    private int $processedCount = 0;

    public function __construct(
        private readonly ConsumerClientInterface $client,
        private readonly HandlerRegistry $registry,
        ?RetryPolicy $retryPolicy = null,
        private readonly ?DlqPublisherInterface $dlqPublisher = null,
        private readonly ?DeduplicatorInterface $deduplicator = null,
        private readonly ?Metrics $metrics = null,
        ?LoggerInterface $logger = null,
        ?callable $sleeper = null
    ) {
        $this->logger = $logger ?? new NullLogger();
        $this->retryEngine = new RetryEngine($retryPolicy ?? RetryPolicy::default(), $sleeper);
    }

    /**
     * Runs the consumer polling loop until stop() is called or the stopCheck returns true.
     *
     * @param (callable(): bool)|null $stopCheck
     */
    public function run(?callable $stopCheck = null): void
    {
        $this->running = true;
        $this->logger->info('Starting StreamForge consumer loop...');

        while ($this->running) {
            // Dispatch pending POSIX signals if pcntl is enabled
            if (function_exists('pcntl_signal_dispatch')) {
                pcntl_signal_dispatch();
            }

            if ($stopCheck !== null && $stopCheck()) {
                $this->logger->info('Stop condition met, terminating consumer loop.');
                break;
            }

            $record = $this->client->poll(500);
            if ($record === null) {
                continue;
            }

            $this->processRecord($record);

            // Periodically trigger garbage collection to prevent memory leaks in CLI daemon
            $this->processedCount++;
            if ($this->processedCount % 100 === 0) {
                gc_collect_cycles();
            }
        }

        $this->logger->info('Consumer loop exited cleanly.');
    }

    /**
     * Processes a single Kafka record through the complete StreamForge operational pipeline.
     */
    public function processRecord(Record $record): bool
    {
        $startTime = microtime(true);

        // 1. Deserialize Event Envelope
        try {
            $event = Event::fromJson($record->value);
        } catch (Throwable $e) {
            $this->logger->error('Received malformed event JSON payload', [
                'topic' => $record->topic,
                'partition' => $record->partition,
                'offset' => $record->offset,
                'error' => $e->getMessage(),
            ]);

            $this->metrics?->recordConsumed($record->topic, 'corrupt');

            // Route poison pill to DLQ if configured
            if ($this->dlqPublisher !== null) {
                $fallbackEvent = Event::create(
                    type: 'malformed.event',
                    source: 'streamforge.consumer',
                    data: ['raw_value' => $record->value]
                );

                $envelope = DlqEnvelope::create(
                    event: $fallbackEvent,
                    topic: $record->topic,
                    partition: $record->partition,
                    offset: $record->offset,
                    errorReason: 'Malformed JSON payload: ' . $e->getMessage(),
                    attempts: 1
                );

                $this->dlqPublisher->publish($envelope);
                $this->metrics?->recordDlq($record->topic, 'malformed_json');
            }

            // Acknowledge offset to prevent poison pill from stalling the partition forever
            $this->client->ack($record);
            return false;
        }

        // 2. Check Idempotency (Deduplication)
        if ($this->deduplicator !== null) {
            if ($this->deduplicator->isDuplicate($event->id)) {
                $this->logger->info('Duplicate event detected, skipping execution', [
                    'event_id' => $event->id,
                    'event_type' => $event->type,
                ]);

                $this->metrics?->recordConsumed($record->topic, 'duplicate');
                $this->client->ack($record);
                return true;
            }
        }

        // 3. Resolve Registered Handler
        $handler = $this->registry->get($event->type);
        if ($handler === null) {
            $this->logger->warning('No handler registered for event type, skipping', [
                'event_type' => $event->type,
                'event_id' => $event->id,
            ]);

            $this->metrics?->recordConsumed($record->topic, 'unhandled');
            $this->client->ack($record);
            return true;
        }

        // 4. Execute with Retry Engine
        $attemptsMade = 0;
        try {
            $this->retryEngine->execute(
                function (int $attempt) use ($handler, $event, &$attemptsMade): void {
                    $attemptsMade = $attempt;
                    $handler->handle($event);
                },
                function (int $attempt, Throwable $e, int $delayMs) use ($record, $event): void {
                    $this->metrics?->recordRetry($record->topic, $event->type);
                    $this->logger->warning('Transient handler failure, retrying', [
                        'event_id' => $event->id,
                        'event_type' => $event->type,
                        'attempt' => $attempt,
                        'delay_ms' => $delayMs,
                        'error' => $e->getMessage(),
                    ]);
                }
            );

            // Record duration and success
            $duration = microtime(true) - $startTime;
            $this->metrics?->recordDuration($record->topic, $event->type, $duration);
            $this->metrics?->recordConsumed($record->topic, 'success');

            $this->client->ack($record);
            return true;
        } catch (Throwable $e) {
            $this->logger->error('Event processing permanently failed', [
                'event_id' => $event->id,
                'event_type' => $event->type,
                'attempts' => $attemptsMade,
                'error' => $e->getMessage(),
            ]);

            if ($this->dlqPublisher !== null) {
                $envelope = DlqEnvelope::create(
                    event: $event,
                    topic: $record->topic,
                    partition: $record->partition,
                    offset: $record->offset,
                    errorReason: $e->getMessage(),
                    attempts: max(1, $attemptsMade)
                );

                $this->dlqPublisher->publish($envelope);
                $this->metrics?->recordDlq($record->topic, $e->getMessage());
                $this->metrics?->recordConsumed($record->topic, 'dlq');

                // Advance offset because message is safely archived in DLQ
                $this->client->ack($record);
                return false;
            }

            $this->metrics?->recordConsumed($record->topic, 'error');
            // Do not ack offset if no DLQ is configured, ensuring at-least-once re-delivery
            throw $e;
        }
    }

    public function stop(): void
    {
        $this->running = false;
    }

    public function isRunning(): bool
    {
        return $this->running;
    }

    public function close(): void
    {
        $this->stop();
        $this->client->close();
    }
}
