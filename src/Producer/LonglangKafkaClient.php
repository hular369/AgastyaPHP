<?php

declare(strict_types=1);

namespace Agastya\Producer;

use longlang\phpkafka\Producer\Producer as KafkaProducer;
use longlang\phpkafka\Producer\ProducerConfig;
use Psr\Log\LoggerInterface;
use Psr\Log\NullLogger;

final class LonglangKafkaClient implements KafkaClientInterface
{
    private KafkaProducer $producer;
    private LoggerInterface $logger;

    /**
     * @param list<string> $bootstrapServers
     */
    public function __construct(
        array $bootstrapServers = ['127.0.0.1:9092'],
        string $clientId = 'agastya-php-producer',
        ?LoggerInterface $logger = null
    ) {
        $this->logger = $logger ?? new NullLogger();

        $config = new ProducerConfig();
        $config->setBootstrapServers($bootstrapServers);
        $config->setClientId($clientId);
        $config->setAcks(-1); // Wait for all in-sync replicas (AllISRAcks)
        $config->setAutoCreateTopic(true);

        $this->producer = new KafkaProducer($config);
    }

    public function send(string $topic, ?string $key, string $value, array $headers = []): void
    {
        // longlang send signature: ($topic, $value, $key = null, $headers = [], $partition = null)
        $this->producer->send($topic, $value, $key, $headers);
    }

    public function close(): void
    {
        $this->producer->close();
    }
}
