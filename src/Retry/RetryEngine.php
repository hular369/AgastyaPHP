<?php

declare(strict_types=1);

namespace StreamForge\Retry;

use StreamForge\Exception\NonRetryableException;
use StreamForge\Exception\RetriesExhaustedException;
use Throwable;

final class RetryEngine
{
    /** @var callable */
    private $sleeper;

    public function __construct(
        private readonly RetryPolicy $policy,
        ?callable $sleeper = null
    ) {
        $this->sleeper = $sleeper ?? static function (int $ms): void {
            usleep($ms * 1000);
        };
    }

    public function calculateDelay(int $attempt): int
    {
        if ($attempt <= 1) {
            return 0;
        }

        $exp = $this->policy->multiplier ** ($attempt - 2);
        $rawDelay = (int) round($this->policy->initialIntervalMs * $exp);

        $delay = min($rawDelay, $this->policy->maxIntervalMs);

        if ($this->policy->jitter && $delay > 0) {
            $half = (int) ($delay / 2);
            $delay = $half + random_int(0, $half);
        }

        return $delay;
    }

    /**
     * Executes the given operation under the configured retry policy.
     *
     * @param callable(int $attempt): mixed $operation
     * @param (callable(int $attempt, Throwable $exception, int $delayMs): void)|null $onRetry
     * @return mixed
     * @throws NonRetryableException
     * @throws RetriesExhaustedException
     */
    public function execute(callable $operation, ?callable $onRetry = null): mixed
    {
        $lastException = null;

        for ($attempt = 1; $attempt <= $this->policy->maxAttempts; $attempt++) {
            try {
                return $operation($attempt);
            } catch (Throwable $e) {
                $lastException = $e;

                // Non-retryable error aborts immediately
                if ($e instanceof NonRetryableException) {
                    throw $e;
                }

                // If last attempt reached, exit loop
                if ($attempt >= $this->policy->maxAttempts) {
                    break;
                }

                $delayMs = $this->calculateDelay($attempt + 1);

                if ($onRetry !== null) {
                    $onRetry($attempt, $e, $delayMs);
                }

                ($this->sleeper)($delayMs);
            }
        }

        throw new RetriesExhaustedException(
            sprintf(
                'Retries exhausted after %d attempts. Last error: %s',
                $this->policy->maxAttempts,
                $lastException?->getMessage() ?? 'unknown error'
            ),
            $this->policy->maxAttempts,
            $lastException
        );
    }
}
