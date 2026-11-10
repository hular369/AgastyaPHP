<?php

declare(strict_types=1);

namespace Agastya\Dlq;

interface DlqPublisherInterface
{
    public function publish(DlqEnvelope $envelope): void;
}
