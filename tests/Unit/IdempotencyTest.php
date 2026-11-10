<?php

declare(strict_types=1);

namespace Agastya\Tests\Unit;

use Agastya\Exception\ValidationException;
use Agastya\Idempotency\MemoryDeduplicator;

final class IdempotencyTest
{
    public function testIsDuplicateLogic(): void
    {
        $dedup = new MemoryDeduplicator(ttlSeconds: 3600);

        // First occurrence: not duplicate
        assert($dedup->isDuplicate('evt-100') === false);

        // Immediate second occurrence: duplicate!
        assert($dedup->isDuplicate('evt-100') === true);

        // Different event: not duplicate
        assert($dedup->isDuplicate('evt-200') === false);

        assert($dedup->size() === 2);
    }

    public function testEmptyEventIdThrows(): void
    {
        $dedup = new MemoryDeduplicator();

        $threw = false;
        try {
            $dedup->isDuplicate('   ');
        } catch (ValidationException) {
            $threw = true;
        }

        assert($threw);
    }

    public function testExpirationSweep(): void
    {
        // 0 second TTL = expires immediately
        $dedup = new MemoryDeduplicator(ttlSeconds: 0);

        $dedup->isDuplicate('evt-short');
        sleep(1);

        $dedup->clean();
        assert($dedup->size() === 0);
    }
}
