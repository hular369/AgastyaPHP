<?php

declare(strict_types=1);

namespace StreamForge\Idempotency;

interface DeduplicatorInterface
{
    /**
     * Checks if an event ID has already been processed.
     * If not seen, records it with the configured TTL and returns false.
     * If already seen and unexpired, returns true.
     */
    public function isDuplicate(string $eventId): bool;

    /**
     * Cleans up expired entries.
     */
    public function clean(): void;
}
