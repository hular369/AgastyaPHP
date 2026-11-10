<?php

declare(strict_types=1);

namespace Agastya\Producer;

use Agastya\Event\Event;

interface ProducerInterface
{
    public function publish(string $topic, Event $event): void;

    public function close(): void;
}
