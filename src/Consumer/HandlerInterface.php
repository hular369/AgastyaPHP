<?php

declare(strict_types=1);

namespace Agastya\Consumer;

use Agastya\Event\Event;

interface HandlerInterface
{
    /**
     * Processes an incoming event.
     * Throw an exception to trigger a retry.
     * Throw a NonRetryableException to bypass retries and send immediately to DLQ.
     */
    public function handle(Event $event): void;
}
