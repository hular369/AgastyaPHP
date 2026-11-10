<?php

declare(strict_types=1);

namespace Agastya\Tests\Unit;

use Agastya\Consumer\HandlerInterface;
use Agastya\Consumer\HandlerRegistry;
use Agastya\Event\Event;
use Agastya\Exception\AgastyaException;

final class HandlerRegistryTest
{
    public function testRegisterAndResolveHandler(): void
    {
        $registry = new HandlerRegistry();
        $called = false;

        $registry->registerCallable('order.created', function (Event $event) use (&$called): void {
            $called = true;
        });

        assert($registry->has('order.created') === true);
        assert($registry->has('payment.completed') === false);

        $handler = $registry->get('order.created');
        assert($handler instanceof HandlerInterface);

        $event = Event::create('order.created', 'orders', ['id' => '1']);
        $handler->handle($event);
        assert($called === true);
    }

    public function testFallbackDefaultHandler(): void
    {
        $registry = new HandlerRegistry();
        $fallbackCalled = false;

        $registry->setDefaultHandler(new class ($fallbackCalled) implements HandlerInterface {
            private $flag;
            public function __construct(&$flag)
            {
                $this->flag = &$flag;
            }
            public function handle(Event $event): void
            {
                $this->flag = true;
            }
        });

        $handler = $registry->get('unregistered.event');
        assert($handler !== null);

        $event = Event::create('unregistered.event', 'test', []);
        $handler->handle($event);
        assert($fallbackCalled === true);
    }

    public function testEmptyEventTypeThrows(): void
    {
        $registry = new HandlerRegistry();
        $threw = false;

        try {
            $registry->registerCallable('', function (): void {});
        } catch (AgastyaException) {
            $threw = true;
        }

        assert($threw);
    }
}
