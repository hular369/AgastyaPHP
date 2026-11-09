<?php

declare(strict_types=1);

namespace StreamForge\Consumer;

use longlang\phpkafka\Consumer\ConsumeMessage;
use longlang\phpkafka\Consumer\Consumer as KafkaConsumer;
use longlang\phpkafka\Consumer\ConsumerConfig;
use Psr\Log\LoggerInterface;
use Psr\Log\NullLogger;
use WeakMap;

final class LonglangConsumerClient implements ConsumerClientInterface
{
    private KafkaConsumer $consumer;
    private LoggerInterface $logger;
    /** @var WeakMap<Record, ConsumeMessage> */
    private WeakMap $messageMap;

    /**
     * @param list<string> $bootstrapServers
     * @param list<string>|string $topics
     */
    public function __construct(
        array $bootstrapServers = ['127.0.0.1:9092'],
        string $groupId = 'streamforge-php-group',
        array|string $topics = 'streamforge.events',
        string $clientId = 'streamforge-php-consumer',
        ?LoggerInterface $logger = null
    ) {
        $this->logger = $logger ?? new NullLogger();
        $this->messageMap = new WeakMap();

        $config = new ConsumerConfig();
        $config->setBootstrapServers($bootstrapServers);
        $config->setGroupId($groupId);
        $config->setTopic(is_array($topics) ? $topics : [$topics]);
        $config->setClientId($clientId);
        $config->setAutoCommit(false); // Manual explicit offset commit on success/DLQ

        $this->consumer = new KafkaConsumer($config);
    }

    public function poll(int $timeoutMs = 1000): ?Record
    {
        $msg = $this->consumer->consume();
        if ($msg === null) {
            return null;
        }

        $headers = [];
        foreach ($msg->getHeaders() as $k => $v) {
            $headers[(string) $k] = (string) $v;
        }

        $record = new Record(
            topic: $msg->getTopic(),
            partition: $msg->getPartition(),
            offset: 0,
            value: $msg->getValue(),
            key: $msg->getKey(),
            headers: $headers
        );

        $this->messageMap[$record] = $msg;
        return $record;
    }

    public function ack(Record $record): void
    {
        if (isset($this->messageMap[$record])) {
            $this->consumer->ack($this->messageMap[$record]);
            unset($this->messageMap[$record]);
        }
    }

    public function close(): void
    {
        $this->consumer->close();
    }
}
