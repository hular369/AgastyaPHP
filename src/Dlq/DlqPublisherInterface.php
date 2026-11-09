<?php

declare(strict_types=1);

namespace StreamForge\Dlq;

interface DlqPublisherInterface
{
    public function publish(DlqEnvelope $envelope): void;
}
