<?php

declare(strict_types=1);

namespace Agastya\Retry;

final readonly class RetryPolicy
{
    public function __construct(
        public int $maxAttempts = 3,
        public int $initialIntervalMs = 100,
        public int $maxIntervalMs = 2000,
        public float $multiplier = 2.0,
        public bool $jitter = true
    ) {
        if ($this->maxAttempts < 1) {
            throw new \InvalidArgumentException('maxAttempts must be >= 1');
        }
        if ($this->initialIntervalMs < 0) {
            throw new \InvalidArgumentException('initialIntervalMs must be >= 0');
        }
        if ($this->maxIntervalMs < $this->initialIntervalMs) {
            throw new \InvalidArgumentException('maxIntervalMs cannot be less than initialIntervalMs');
        }
        if ($this->multiplier < 1.0) {
            throw new \InvalidArgumentException('multiplier must be >= 1.0');
        }
    }

    public static function default(): self
    {
        return new self();
    }
}
