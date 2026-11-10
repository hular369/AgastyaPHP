<?php

declare(strict_types=1);

namespace Agastya\Tests\Unit;

use Exception;
use Agastya\Exception\NonRetryableException;
use Agastya\Exception\RetriesExhaustedException;
use Agastya\Retry\RetryEngine;
use Agastya\Retry\RetryPolicy;

final class RetryEngineTest
{
    public function testImmediateSuccess(): void
    {
        $policy = new RetryPolicy(maxAttempts: 3);
        $engine = new RetryEngine($policy);

        $calls = 0;
        $result = $engine->execute(function (int $attempt) use (&$calls) {
            $calls++;
            return 'success-' . $attempt;
        });

        assert($result === 'success-1');
        assert($calls === 1);
    }

    public function testSuccessAfterRetries(): void
    {
        $policy = new RetryPolicy(
            maxAttempts: 4,
            initialIntervalMs: 10,
            maxIntervalMs: 50,
            multiplier: 2.0,
            jitter: false
        );

        $sleptDelays = [];
        $engine = new RetryEngine($policy, function (int $ms) use (&$sleptDelays): void {
            $sleptDelays[] = $ms;
        });

        $calls = 0;
        $retries = 0;

        $result = $engine->execute(
            function (int $attempt) use (&$calls) {
                $calls++;
                if ($attempt < 3) {
                    throw new Exception('Transient network error');
                }
                return 'done';
            },
            function (int $attempt, \Throwable $e, int $delayMs) use (&$retries): void {
                $retries++;
            }
        );

        assert($result === 'done');
        assert($calls === 3);
        assert($retries === 2);
        assert(count($sleptDelays) === 2);
        assert($sleptDelays[0] === 10);
        assert($sleptDelays[1] === 20);
    }

    public function testRetriesExhaustedThrows(): void
    {
        $policy = new RetryPolicy(maxAttempts: 3, initialIntervalMs: 1);
        $engine = new RetryEngine($policy, static function (): void {});

        $calls = 0;
        $threw = false;

        try {
            $engine->execute(function () use (&$calls) {
                $calls++;
                throw new Exception('Permanent failure');
            });
        } catch (RetriesExhaustedException $e) {
            $threw = true;
            assert($e->getAttemptsMade() === 3);
            assert(str_contains($e->getMessage(), 'Permanent failure'));
        }

        assert($threw, 'Should throw RetriesExhaustedException');
        assert($calls === 3);
    }

    public function testNonRetryableExceptionAbortsImmediately(): void
    {
        $policy = new RetryPolicy(maxAttempts: 5, initialIntervalMs: 1);
        $engine = new RetryEngine($policy, static function (): void {
            throw new Exception('Sleeper should not be called');
        });

        $calls = 0;
        $threw = false;

        try {
            $engine->execute(function () use (&$calls) {
                $calls++;
                throw new NonRetryableException('Invalid schema data');
            });
        } catch (NonRetryableException $e) {
            $threw = true;
            assert($e->getMessage() === 'Invalid schema data');
        }

        assert($threw);
        assert($calls === 1, 'Non-retryable exception must not be retried');
    }

    public function testCalculateDelayWithCap(): void
    {
        $policy = new RetryPolicy(
            maxAttempts: 5,
            initialIntervalMs: 100,
            maxIntervalMs: 500,
            multiplier: 2.0,
            jitter: false
        );
        $engine = new RetryEngine($policy);

        assert($engine->calculateDelay(1) === 0);
        assert($engine->calculateDelay(2) === 100);
        assert($engine->calculateDelay(3) === 200);
        assert($engine->calculateDelay(4) === 400);
        assert($engine->calculateDelay(5) === 500); // Capped at maxIntervalMs
    }
}
