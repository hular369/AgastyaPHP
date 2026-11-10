<?php

declare(strict_types=1);

namespace Agastya\Event;

use DateTimeImmutable;
use DateTimeInterface;
use JsonSerializable;
use Ramsey\Uuid\Uuid;
use Agastya\Exception\ValidationException;

/**
 * CloudEvents v1.0 compliant event envelope.
 */
final readonly class Event implements JsonSerializable
{
    public const DEFAULT_SPEC_VERSION = '1.0';
    public const DEFAULT_DATA_CONTENT_TYPE = 'application/json';

    public function __construct(
        public string $id,
        public string $type,
        public string $source,
        public array $data,
        public DateTimeImmutable $time,
        public ?string $key = null,
        public string $specversion = self::DEFAULT_SPEC_VERSION,
        public string $datacontenttype = self::DEFAULT_DATA_CONTENT_TYPE,
        public array $metadata = []
    ) {
        $this->validate();
    }

    public static function create(
        string $type,
        string $source,
        array $data,
        ?string $key = null,
        array $metadata = [],
        ?DateTimeImmutable $time = null
    ): self {
        return new self(
            id: Uuid::uuid4()->toString(),
            type: trim($type),
            source: trim($source),
            data: $data,
            time: $time ?? new DateTimeImmutable('now', new \DateTimeZone('UTC')),
            key: $key !== null ? trim($key) : null,
            metadata: $metadata
        );
    }

    public function validate(): void
    {
        if (empty($this->id)) {
            throw new ValidationException('Event id cannot be empty.');
        }
        if (empty($this->type)) {
            throw new ValidationException('Event type cannot be empty.');
        }
        if (empty($this->source)) {
            throw new ValidationException('Event source cannot be empty.');
        }
        if (empty($this->specversion)) {
            throw new ValidationException('Event specversion cannot be empty.');
        }
    }

    public function withMetadata(string $key, string $value): self
    {
        $meta = $this->metadata;
        $meta[$key] = $value;

        return new self(
            id: $this->id,
            type: $this->type,
            source: $this->source,
            data: $this->data,
            time: $this->time,
            key: $this->key,
            specversion: $this->specversion,
            datacontenttype: $this->datacontenttype,
            metadata: $meta
        );
    }

    public function toArray(): array
    {
        $array = [
            'id' => $this->id,
            'specversion' => $this->specversion,
            'type' => $this->type,
            'source' => $this->source,
            'datacontenttype' => $this->datacontenttype,
            'time' => $this->time->format(DateTimeInterface::RFC3339_EXTENDED),
            'data' => $this->data,
        ];

        if ($this->key !== null && $this->key !== '') {
            $array['key'] = $this->key;
        }

        if (!empty($this->metadata)) {
            $array['metadata'] = $this->metadata;
        }

        return $array;
    }

    public function toJson(): string
    {
        $json = json_encode($this->toArray(), JSON_UNESCAPED_SLASHES | JSON_THROW_ON_ERROR);
        return $json;
    }

    public function jsonSerialize(): array
    {
        return $this->toArray();
    }

    public static function fromArray(array $payload): self
    {
        if (!isset($payload['id'], $payload['type'], $payload['source'], $payload['data'])) {
            throw new ValidationException('Payload is missing required CloudEvents fields (id, type, source, data).');
        }

        $time = isset($payload['time'])
            ? new DateTimeImmutable((string) $payload['time'])
            : new DateTimeImmutable('now', new \DateTimeZone('UTC'));

        return new self(
            id: (string) $payload['id'],
            type: (string) $payload['type'],
            source: (string) $payload['source'],
            data: (array) $payload['data'],
            time: $time,
            key: isset($payload['key']) ? (string) $payload['key'] : null,
            specversion: (string) ($payload['specversion'] ?? self::DEFAULT_SPEC_VERSION),
            datacontenttype: (string) ($payload['datacontenttype'] ?? self::DEFAULT_DATA_CONTENT_TYPE),
            metadata: (array) ($payload['metadata'] ?? [])
        );
    }

    public static function fromJson(string $json): self
    {
        $decoded = json_decode($json, true, 512, JSON_THROW_ON_ERROR);
        if (!is_array($decoded)) {
            throw new ValidationException('Invalid JSON payload: expected JSON object.');
        }

        return self::fromArray($decoded);
    }
}
