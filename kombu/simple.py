"""Simple messaging interface."""

from __future__ import annotations

import socket
from collections import deque
from contextlib import contextmanager
from queue import Empty
from time import monotonic
from typing import TYPE_CHECKING

from . import entity, messaging
from .connection import maybe_channel
from .consume_ledger import (
    KIND_LATCH,
    KIND_SIMPLE_CLOSE,
    KIND_SIMPLE_GET,
    KIND_SIMPLE_GET_NOWAIT,
    PLANE_SIMPLE,
    STATUS_COMMITTED,
    get_ledger,
    ledger_for,
)

if TYPE_CHECKING:
    from types import TracebackType

__all__ = ('SimpleQueue', 'SimpleBuffer')


class SimpleBase:
    Empty = Empty
    _consuming = False

    def __enter__(self):
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc_val: BaseException | None,
        exc_tb: TracebackType | None
    ) -> None:
        self.close()

    def __init__(self, channel, producer, consumer, no_ack=False):
        self.channel = maybe_channel(channel)
        self.producer = producer
        self.consumer = consumer
        self.no_ack = no_ack
        self.queue = self.consumer.queues[0]
        self.buffer = deque()
        self.consumer.register_callback(self._receive)

    def consume_ledger(self):
        """Open a consumption registration & cancellation commit scope on
        the channel of this simple queue/buffer.

        Yields the :class:`~kombu.consume_ledger.ConsumeLedger` (the frame
        is available as ``ledger.top``).
        """
        @contextmanager
        def _scope():
            ledger = get_ledger(self.channel)
            ledger.register_owner(self)
            ledger.register_owner(self.consumer)
            with ledger.scope():
                yield ledger
        return _scope()

    def _ledger_queue_names(self):
        # Owner protocol for ConsumeLedger frame snapshots.
        return set(self.consumer._queues)

    def _ledger_latch(self):
        return bool(self._consuming)

    def get(self, block=True, timeout=None):
        if not block:
            return self.get_nowait()

        self._consume()

        ledger = ledger_for(self.channel)
        if ledger is not None:
            ledger.record(
                plane=PLANE_SIMPLE, kind=KIND_SIMPLE_GET, owner=id(self),
                queue=self.queue.name, status=STATUS_COMMITTED,
                path='drain_events', latch=bool(self._consuming),
            )

        time_start = monotonic()
        remaining = timeout
        while True:
            if self.buffer:
                return self.buffer.popleft()

            if remaining is not None and remaining <= 0.0:
                raise self.Empty()

            try:
                # The `drain_events` method will
                # block on the socket connection to rabbitmq. if any
                # application-level messages are received, it will put them
                # into `self.buffer`.
                # * The method will block for UP TO `timeout` milliseconds.
                # * The method may raise a socket.timeout exception; or...
                # * The method may return without having put anything on
                #    `self.buffer`.  This is because internal heartbeat
                #    messages are sent over the same socket; also POSIX makes
                #    no guarantees against socket calls returning early.
                self.channel.connection.client.drain_events(timeout=remaining)
            except socket.timeout:
                raise self.Empty()

            if remaining is not None:
                elapsed = monotonic() - time_start
                remaining = timeout - elapsed

    def get_nowait(self):
        ledger = ledger_for(self.channel)
        if ledger is not None:
            ledger.register_owner(self)
            # basic_get path: it must never register a consumer tag.
            ledger.record(
                plane=PLANE_SIMPLE, kind=KIND_SIMPLE_GET_NOWAIT,
                owner=id(self), queue=self.queue.name,
                status=STATUS_COMMITTED, path='basic_get',
                latch=bool(self._consuming),
            )
        m = self.queue.get(no_ack=self.no_ack, accept=self.consumer.accept)
        if not m:
            raise self.Empty()
        return m

    def put(self, message, serializer=None, headers=None, compression=None,
            routing_key=None, **kwargs):
        self.producer.publish(message,
                              serializer=serializer,
                              routing_key=routing_key,
                              headers=headers,
                              compression=compression,
                              **kwargs)

    def clear(self):
        return self.consumer.purge()

    def qsize(self):
        _, size, _ = self.queue.queue_declare(passive=True)
        return size

    def close(self):
        ledger = ledger_for(self.channel)
        if ledger is not None:
            ledger.register_owner(self)
            # The latch is deliberately never reset by close: a second
            # consumer would need a fresh SimpleBase.  Journal that.
            ledger.record(
                plane=PLANE_SIMPLE, kind=KIND_SIMPLE_CLOSE,
                owner=id(self), queue=self.queue.name,
                status=STATUS_COMMITTED,
                latch_was=bool(self._consuming), latch_reset=False,
            )
        self.consumer.cancel()

    def _receive(self, message_data, message):
        self.buffer.append(message)

    def _consume(self):
        ledger = ledger_for(self.channel)
        if ledger is not None:
            ledger.register_owner(self)
            ledger.register_owner(self.consumer)
        if not self._consuming:
            self.consumer.consume(no_ack=self.no_ack)
            self._consuming = True
            if ledger is not None:
                ledger.record(
                    plane=PLANE_SIMPLE, kind=KIND_LATCH, owner=id(self),
                    queue=self.queue.name, status=STATUS_COMMITTED,
                    phase='set',
                )
        elif ledger is not None:
            # One-shot latch: no new basic_consume/registration happens.
            ledger.record(
                plane=PLANE_SIMPLE, kind=KIND_LATCH, owner=id(self),
                queue=self.queue.name, status=STATUS_COMMITTED,
                phase='reused',
            )

    def __len__(self):
        """`len(self) -> self.qsize()`."""
        return self.qsize()

    def __bool__(self):
        return True
    __nonzero__ = __bool__


class SimpleQueue(SimpleBase):
    """Simple API for persistent queues."""

    no_ack = False
    queue_opts = {}
    queue_args = {}
    exchange_opts = {'type': 'direct'}

    def __init__(self, channel, name, no_ack=None, queue_opts=None,
                 queue_args=None, exchange_opts=None, serializer=None,
                 compression=None, accept=None):
        queue = name
        queue_opts = dict(self.queue_opts, **queue_opts or {})
        queue_args = dict(self.queue_args, **queue_args or {})
        exchange_opts = dict(self.exchange_opts, **exchange_opts or {})
        if no_ack is None:
            no_ack = self.no_ack
        if not isinstance(queue, entity.Queue):
            exchange = entity.Exchange(name, **exchange_opts)
            queue = entity.Queue(name, exchange, name,
                                 queue_arguments=queue_args,
                                 **queue_opts)
            routing_key = name
        else:
            exchange = queue.exchange
            routing_key = queue.routing_key
        consumer = messaging.Consumer(channel, queue, accept=accept)
        producer = messaging.Producer(channel, exchange,
                                      serializer=serializer,
                                      routing_key=routing_key,
                                      compression=compression)
        super().__init__(channel, producer,
                         consumer, no_ack)


class SimpleBuffer(SimpleQueue):
    """Simple API for ephemeral queues."""

    no_ack = True
    queue_opts = {'durable': False,
                  'auto_delete': True}
    exchange_opts = {'durable': False,
                     'delivery_mode': 'transient',
                     'auto_delete': True}
