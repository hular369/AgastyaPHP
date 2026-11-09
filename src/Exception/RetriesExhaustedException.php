<?php

declare(strict_types=1);

namespace StreamForge\Exception;

use Throwable;

class RetriesExhaustedException extends StreamForgeException
{
    public function __construct(
        string $message,
        private readonly int $attemptsMade,
        ?Throwable $previous = null
    ) {
        parent::__construct($message, 0, $previous);
    }

    public function getAttemptsMade(): int
    {
        return $this->attemptsMade;
    }
}
