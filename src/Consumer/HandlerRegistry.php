<?php

declare(strict_types=1);

namespace Agastya\Consumer;

use Agastya\Event\Event;
use Agastya\Exception\AgastyaException;

final class HandlerRegistry
{
    /** @var array<string, HandlerInterface> */
    private array $handlers = [];
    private ?HandlerInterface $defaultHandler = null;

    public function register(string $eventType, HandlerInterface $handler): self
    {
        if (trim($eventType) === '') {
            throw new AgastyaException('Event type cannot be empty.');
        }

        $this->handlers[$eventType] = $handler;
        return $this;
    }

    public function registerCallable(string $eventType, callable $callable): self
    {
        return $this->register($eventType, new class ($callable) implements HandlerInterface {
            /** @var callable */
            private $callable;

            public function __construct(callable $callable)
            {
                $this->callable = $callable;
            }

            public function handle(Event $event): void
            {
                ($this->callable)($event);
            }
        });
    }

    public function setDefaultHandler(HandlerInterface $handler): self
    {
        $this->defaultHandler = $handler;
        return $this;
    }

    public function get(string $eventType): ?HandlerInterface
    {
        return $this->handlers[$eventType] ?? $this->defaultHandler;
    }

    public function has(string $eventType): bool
    {
        return isset($this->handlers[$eventType]) || $this->defaultHandler !== null;
    }

    /**
     * @return array<string, HandlerInterface>
     */
    public function all(): array
    {
        return $this->handlers;
    }
}
