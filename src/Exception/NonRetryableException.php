<?php

declare(strict_types=1);

namespace StreamForge\Exception;

/**
 * Thrown when an error is permanent (e.g. invalid schema, corrupt payload)
 * and retrying would be futile, routing directly to the Dead Letter Queue.
 */
class NonRetryableException extends StreamForgeException
{
}
