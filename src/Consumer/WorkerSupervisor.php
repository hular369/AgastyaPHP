<?php

declare(strict_types=1);

namespace StreamForge\Consumer;

use Psr\Log\LoggerInterface;
use Psr\Log\NullLogger;
use StreamForge\Observability\Metrics;

/**
 * Multi-process worker supervisor for concurrent event processing in PHP.
 * Uses pcntl_fork() and POSIX signal management for self-healing, reliable worker pools.
 */
final class WorkerSupervisor
{
    private LoggerInterface $logger;
    /** @var array<int, int> PID => restartCount */
    private array $workers = [];
    private bool $running = false;

    /**
     * @param callable(): Consumer $workerFactory Factory returning a ready-to-run Consumer instance
     */
    public function __construct(
        private readonly int $workerCount,
        private $workerFactory,
        private readonly string $consumerGroup = 'streamforge-php-group',
        private readonly ?Metrics $metrics = null,
        ?LoggerInterface $logger = null
    ) {
        $this->logger = $logger ?? new NullLogger();
    }

    public function run(): void
    {
        if (!function_exists('pcntl_fork')) {
            $this->logger->warning('pcntl extension not available, running in single-process mode');
            $consumer = ($this->workerFactory)();
            $consumer->run();
            return;
        }

        $this->running = true;
        $this->registerSignals();

        $this->logger->info('Starting StreamForge Worker Supervisor', [
            'supervisor_pid' => getmypid(),
            'worker_count' => $this->workerCount,
            'consumer_group' => $this->consumerGroup,
        ]);

        // Spawn initial worker pool
        for ($i = 0; $i < $this->workerCount; $i++) {
            $this->spawnWorker();
        }

        $this->metrics?->setActiveWorkers($this->consumerGroup, count($this->workers));

        // Supervisor monitoring loop
        while ($this->running) {
            pcntl_signal_dispatch();

            // Check if any child process has exited
            $pid = pcntl_waitpid(-1, $status, WNOHANG);

            if ($pid > 0) {
                $exitCode = pcntl_wexitstatus($status);
                unset($this->workers[$pid]);
                $this->metrics?->setActiveWorkers($this->consumerGroup, count($this->workers));

                $this->logger->warning('Worker process terminated', [
                    'worker_pid' => $pid,
                    'exit_code' => $exitCode,
                    'remaining_workers' => count($this->workers),
                ]);

                // Respawn worker if supervisor is still actively running
                if ($this->running) {
                    $this->logger->info('Respawning replacement worker process...');
                    $this->spawnWorker();
                    $this->metrics?->setActiveWorkers($this->consumerGroup, count($this->workers));
                }
            }

            usleep(100000); // 100ms supervisor sleep tick
        }

        $this->shutdownChildren();
        $this->logger->info('All worker processes drained and terminated cleanly.');
    }

    private function spawnWorker(): void
    {
        $pid = pcntl_fork();

        if ($pid === -1) {
            $this->logger->error('Failed to fork worker process');
            return;
        }

        if ($pid === 0) {
            // Child process execution
            $this->runChild();
            exit(0);
        }

        // Parent process tracks child PID
        $this->workers[$pid] = 0;
        $this->logger->info('Spawned worker process', ['worker_pid' => $pid]);
    }

    private function runChild(): void
    {
        $childPid = getmypid();
        $this->logger->info('Child worker process initialized', ['pid' => $childPid]);

        // Reset child signal handlers
        $consumer = ($this->workerFactory)();

        pcntl_signal(SIGTERM, static function () use ($consumer, $childPid): void {
            $consumer->stop();
        });
        pcntl_signal(SIGINT, static function () use ($consumer, $childPid): void {
            $consumer->stop();
        });

        try {
            $consumer->run();
        } catch (\Throwable $e) {
            $this->logger->error('Worker process crashed with exception', [
                'pid' => $childPid,
                'error' => $e->getMessage(),
            ]);
            exit(1);
        }

        $consumer->close();
        exit(0);
    }

    private function registerSignals(): void
    {
        $handler = function (int $signal): void {
            $this->logger->info('Supervisor received shutdown signal', ['signal' => $signal]);
            $this->running = false;
        };

        pcntl_signal(SIGTERM, $handler);
        pcntl_signal(SIGINT, $handler);
        pcntl_signal(SIGHUP, $handler);
    }

    private function shutdownChildren(): void
    {
        $this->logger->info('Broadcasting SIGTERM to all worker children...');

        foreach (array_keys($this->workers) as $pid) {
            posix_kill($pid, SIGTERM);
        }

        // Wait for all children to exit
        while (!empty($this->workers)) {
            $pid = pcntl_waitpid(-1, $status, 0);
            if ($pid > 0) {
                unset($this->workers[$pid]);
            }
        }

        $this->metrics?->setActiveWorkers($this->consumerGroup, 0);
    }
}
