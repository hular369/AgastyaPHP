<?php

declare(strict_types=1);

namespace Agastya\Tests\Unit;

use Agastya\Observability\Metrics;

final class MetricsTest
{
    public function testRecordAndRenderMetrics(): void
    {
        $metrics = new Metrics();

        $metrics->recordPublished('orders.v1', 'success');
        $metrics->recordConsumed('orders.v1', 'success');
        $metrics->recordDuration('orders.v1', 'order.created', 0.045);
        $metrics->recordRetry('orders.v1', 'order.created');
        $metrics->recordDlq('orders.v1', 'timeout');
        $metrics->setActiveWorkers('orders-group', 4);

        $output = $metrics->render();

        assert(str_contains($output, 'agastya_events_published_total'));
        assert(str_contains($output, 'agastya_events_consumed_total'));
        assert(str_contains($output, 'agastya_event_processing_duration_seconds'));
        assert(str_contains($output, 'agastya_event_retries_total'));
        assert(str_contains($output, 'agastya_dlq_events_total'));
        assert(str_contains($output, 'agastya_active_workers'));
        assert(str_contains($output, 'consumer_group="orders-group"} 4'));
    }
}
