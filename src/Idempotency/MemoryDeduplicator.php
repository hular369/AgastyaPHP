<?php

declare(strict_types=1);

namespace StreamForge\Idempotency;

use StreamForge\Exception\ValidationException;

final class MemoryDeduplicator implements DeduplicatorInterface
{
    /** @var array<string, int> eventId => expiresAt timestamp */
    private array $store = [];

    public function __construct(
        private readonly int $ttlSeconds = 86400 // Default 24 hours
    ) {
    }

    public function isDuplicate(string $eventId): bool
    {
        if (trim($eventId) === '') {
            throw new ValidationException('Event ID cannot be empty for deduplication.');
        }

        $now = time();

        // Check if exists and not expired
        if (isset($this->store[$eventId]) && $this->store[$eventId] > $now) {
            return true;
        }

        // Record new entry
        $this->store[$eventId] = $now + $this->ttlSeconds;

        // Opportunistic cleanup if store grows large
        if (count($this->store) > 1000) {
            $this->clean();
        }

        return false;
    }

    public function clean(): void
    {
        $now = time();
        foreach ($this->store as $id => $expiresAt) {
            if ($expiresAt <= $now) {
                unset($this->store[$id]);
            }
        }
    }

    public function size(): int
    {
        return count($this->store);
    }
}
