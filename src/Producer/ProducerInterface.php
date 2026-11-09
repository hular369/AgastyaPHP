<?php

declare(strict_types=1);

namespace StreamForge\Producer;

use StreamForge\Event\Event;

interface ProducerInterface
{
    public function publish(string $topic, Event $event): void;

    public function close(): void;
}
