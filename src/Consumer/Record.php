<?php

declare(strict_types=1);

namespace StreamForge\Consumer;

final readonly class Record
{
    /**
     * @param array<string, string> $headers
     */
    public function __construct(
        public string $topic,
        public int $partition,
        public int $offset,
        public string $value,
        public ?string $key = null,
        public array $headers = []
    ) {
    }
}
