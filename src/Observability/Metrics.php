<?php

declare(strict_types=1);

namespace Agastya\Observability;

use Prometheus\CollectorRegistry;
use Prometheus\Counter;
use Prometheus\Gauge;
use Prometheus\Histogram;
use Prometheus\RenderTextFormat;
use Prometheus\Storage\InMemory;

final class Metrics
{
    private CollectorRegistry $registry;
    private Counter $eventsPublishedTotal;
    private Counter $eventsConsumedTotal;
    private Histogram $eventProcessingDuration;
    private Counter $eventRetriesTotal;
    private Counter $dlqEventsTotal;
    private Gauge $activeWorkers;

    public function __construct(?CollectorRegistry $registry = null)
    {
        $this->registry = $registry ?? new CollectorRegistry(new InMemory());

        $this->eventsPublishedTotal = $this->registry->getOrRegisterCounter(
            'agastya',
            'events_published_total',
            'Total count of published events',
            ['topic', 'status']
        );

        $this->eventsConsumedTotal = $this->registry->getOrRegisterCounter(
            'agastya',
            'events_consumed_total',
            'Total count of consumed events',
            ['topic', 'status']
        );

        $this->eventProcessingDuration = $this->registry->getOrRegisterHistogram(
            'agastya',
            'event_processing_duration_seconds',
            'Duration of event handler execution in seconds',
            ['topic', 'event_type'],
            [0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0]
        );

        $this->eventRetriesTotal = $this->registry->getOrRegisterCounter(
            'agastya',
            'event_retries_total',
            'Total count of transient failure retry attempts',
            ['topic', 'event_type']
        );

        $this->dlqEventsTotal = $this->registry->getOrRegisterCounter(
            'agastya',
            'dlq_events_total',
            'Total count of messages routed to the Dead Letter Queue',
            ['topic', 'reason']
        );

        $this->activeWorkers = $this->registry->getOrRegisterGauge(
            'agastya',
            'active_workers',
            'Current count of active worker processes',
            ['consumer_group']
        );
    }

    public function recordPublished(string $topic, string $status = 'success'): void
    {
        $this->eventsPublishedTotal->inc(['topic' => $topic, 'status' => $status]);
    }

    public function recordConsumed(string $topic, string $status = 'success'): void
    {
        $this->eventsConsumedTotal->inc(['topic' => $topic, 'status' => $status]);
    }

    public function recordDuration(string $topic, string $eventType, float $seconds): void
    {
        $this->eventProcessingDuration->observe($seconds, ['topic' => $topic, 'event_type' => $eventType]);
    }

    public function recordRetry(string $topic, string $eventType): void
    {
        $this->eventRetriesTotal->inc(['topic' => $topic, 'event_type' => $eventType]);
    }

    public function recordDlq(string $topic, string $reason): void
    {
        // Sanitize reason for label to prevent high cardinality
        $sanitizedReason = substr(preg_replace('/[^a-zA-Z0-9_-]/', '_', $reason) ?? 'unknown', 0, 32);
        $this->dlqEventsTotal->inc(['topic' => $topic, 'reason' => $sanitizedReason]);
    }

    public function setActiveWorkers(string $group, int $count): void
    {
        $this->activeWorkers->set($count, ['consumer_group' => $group]);
    }

    public function render(): string
    {
        $renderer = new RenderTextFormat();
        return $renderer->render($this->registry->getMetricFamilySamples());
    }

    public function getRegistry(): CollectorRegistry
    {
        return $this->registry;
    }
}
