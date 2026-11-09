<?php

declare(strict_types=1);

namespace StreamForge;

use Psr\Log\LoggerInterface;
use Psr\Log\NullLogger;
use StreamForge\Consumer\Consumer;
use StreamForge\Consumer\ConsumerClientInterface;
use StreamForge\Consumer\HandlerInterface;
use StreamForge\Consumer\HandlerRegistry;
use StreamForge\Consumer\LonglangConsumerClient;
use StreamForge\Consumer\WorkerSupervisor;
use StreamForge\Dlq\DlqPublisher;
use StreamForge\Dlq\DlqPublisherInterface;
use StreamForge\Event\Event;
use StreamForge\Idempotency\DeduplicatorInterface;
use StreamForge\Idempotency\MemoryDeduplicator;
use StreamForge\Observability\Metrics;
use StreamForge\Producer\KafkaClientInterface;
use StreamForge\Producer\LonglangKafkaClient;
use StreamForge\Producer\Producer;
use StreamForge\Producer\ProducerInterface;
use StreamForge\Retry\RetryPolicy;

/**
 * Public facade and factory for StreamForge event processing.
 */
final class StreamForge
{
    private HandlerRegistry $registry;
    private Metrics $metrics;
    private LoggerInterface $logger;
    private ?DeduplicatorInterface $deduplicator = null;
    private ?DlqPublisherInterface $dlqPublisher = null;
    private ?ProducerInterface $dlqProducer = null;
    private RetryPolicy $retryPolicy;

    /**
     * @param array<string, mixed> $config
     */
    public function __construct(
        private readonly array $config = [],
        ?LoggerInterface $logger = null,
        ?Metrics $metrics = null
    ) {
        $this->logger = $logger ?? new NullLogger();
        $this->metrics = $metrics ?? new Metrics();
        $this->registry = new HandlerRegistry();

        $this->configureRetry();
        $this->configureDeduplication();
        $this->configureDlq();
    }

    /**
     * @param array<string, mixed> $config
     */
    public static function create(
        array $config = [],
        ?LoggerInterface $logger = null,
        ?Metrics $metrics = null
    ): self {
        return new self($config, $logger, $metrics);
    }

    /**
     * @param array<string, mixed> $config
     */
    public static function createProducer(
        array $config = [],
        ?KafkaClientInterface $client = null,
        ?Metrics $metrics = null,
        ?LoggerInterface $logger = null
    ): ProducerInterface {
        $brokers = $config['brokers'] ?? ['127.0.0.1:9092'];
        $clientId = $config['client_id'] ?? 'streamforge-php-producer';

        $kafkaClient = $client ?? new LonglangKafkaClient($brokers, $clientId, $logger);
        return new Producer($kafkaClient, $metrics, $logger);
    }

    public function register(string $eventType, HandlerInterface $handler): self
    {
        $this->registry->register($eventType, $handler);
        return $this;
    }

    public function registerCallable(string $eventType, callable $callable): self
    {
        $this->registry->registerCallable($eventType, $callable);
        return $this;
    }

    public function setDefaultHandler(HandlerInterface $handler): self
    {
        $this->registry->setDefaultHandler($handler);
        return $this;
    }

    public function createConsumer(?ConsumerClientInterface $client = null): Consumer
    {
        $brokers = $this->config['brokers'] ?? ['127.0.0.1:9092'];
        $groupId = $this->config['group_id'] ?? 'streamforge-php-group';
        $topics = $this->config['topics'] ?? ['streamforge.events'];
        $clientId = $this->config['client_id'] ?? 'streamforge-php-consumer';

        $consumerClient = $client ?? new LonglangConsumerClient(
            bootstrapServers: $brokers,
            groupId: $groupId,
            topics: $topics,
            clientId: $clientId,
            logger: $this->logger
        );

        return new Consumer(
            client: $consumerClient,
            registry: $this->registry,
            retryPolicy: $this->retryPolicy,
            dlqPublisher: $this->dlqPublisher,
            deduplicator: $this->deduplicator,
            metrics: $this->metrics,
            logger: $this->logger
        );
    }

    public function run(): void
    {
        $workers = (int) ($this->config['workers'] ?? 1);
        $groupId = $this->config['group_id'] ?? 'streamforge-php-group';

        if ($workers > 1 && function_exists('pcntl_fork')) {
            $supervisor = new WorkerSupervisor(
                workerCount: $workers,
                workerFactory: fn() => $this->createConsumer(),
                consumerGroup: $groupId,
                metrics: $this->metrics,
                logger: $this->logger
            );
            $supervisor->run();
        } else {
            $consumer = $this->createConsumer();
            $consumer->run();
        }
    }

    public function getMetrics(): Metrics
    {
        return $this->metrics;
    }

    public function getRegistry(): HandlerRegistry
    {
        return $this->registry;
    }

    private function configureRetry(): void
    {
        $retryCfg = $this->config['retry'] ?? [];
        $this->retryPolicy = new RetryPolicy(
            maxAttempts: (int) ($retryCfg['max_attempts'] ?? 3),
            initialIntervalMs: (int) ($retryCfg['initial_interval_ms'] ?? 100),
            maxIntervalMs: (int) ($retryCfg['max_interval_ms'] ?? 2000),
            multiplier: (float) ($retryCfg['multiplier'] ?? 2.0),
            jitter: (bool) ($retryCfg['jitter'] ?? true)
        );
    }

    private function configureDeduplication(): void
    {
        $enableDedup = $this->config['deduplication'] ?? true;
        if ($enableDedup) {
            $ttl = (int) ($this->config['deduplication_ttl'] ?? 86400);
            $this->deduplicator = new MemoryDeduplicator($ttl);
        }
    }

    private function configureDlq(): void
    {
        $enableDlq = $this->config['dlq'] ?? true;
        if ($enableDlq) {
            $brokers = $this->config['brokers'] ?? ['127.0.0.1:9092'];
            $clientId = ($this->config['client_id'] ?? 'streamforge-php') . '-dlq-producer';

            $client = new LonglangKafkaClient($brokers, $clientId, $this->logger);
            $this->dlqProducer = new Producer($client, $this->metrics, $this->logger);

            $this->dlqPublisher = new DlqPublisher(
                function (string $topic, ?string $key, string $payload, array $headers): void {
                    $dlqEvent = Event::create(
                        type: 'dlq.envelope',
                        source: 'streamforge.dlq',
                        data: json_decode($payload, true, 512, JSON_THROW_ON_ERROR),
                        key: $key,
                        metadata: $headers
                    );
                    $this->dlqProducer->publish($topic, $dlqEvent);
                },
                $this->logger
            );
        }
    }
}
