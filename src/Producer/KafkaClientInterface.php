<?php

declare(strict_types=1);

namespace Agastya\Producer;

interface KafkaClientInterface
{
    /**
     * Publishes a message to an Apache Kafka topic.
     *
     * @param string $topic
     * @param string|null $key Partition key
     * @param string $value Serialized message payload
     * @param array<string, string> $headers Key-value message headers
     */
    public function send(string $topic, ?string $key, string $value, array $headers = []): void;

    /**
     * Closes the client and flushes any pending buffers.
     */
    public function close(): void;
}
