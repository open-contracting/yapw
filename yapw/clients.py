"""
Mixins that can be combined to create a RabbitMQ client. For example:

.. code-block:: python

   from yapw import clients

   class Client(clients.Threaded, clients.Durable, clients.Blocking, clients.Base):
       pass

The layers are:

Base
  For common logic, without interacting with RabbitMQ.

  Available mixins:

  -  :class:`~yapw.clients.Base`
Connection
  Establish a connection to RabbitMQ and create a channel.

  Available mixins:

  -  :class:`~yapw.clients.Blocking`
Publisher
  Declare an exchange, declare and bind queues, and publish messages.

  Available mixins:

  -  :class:`~yapw.clients.Durable`
  -  :class:`~yapw.clients.Transient`
Consumer
  Consume messages.

  Available mixins:

  -  :class:`~yapw.clients.Threaded`

.. note::

   Importing this module sets the level of the "pika" logger to ``WARNING``, so that consumers can use the ``DEBUG``
   and ``INFO`` levels without their messages getting lost in Pika's verbosity.
"""
from __future__ import annotations

import functools
import logging
import signal
import threading
from collections import namedtuple
from types import FrameType
from typing import Any, Callable, List, Optional, Set, Tuple

import pika
from pika.exchange_type import ExchangeType

from yapw.decorators import default_decode, halt
from yapw.ossignal import install_signal_handlers, signal_names
from yapw.types import ConsumerCallback, Decode, Decorator, Encode, State
from yapw.util import basic_publish_debug_args, basic_publish_kwargs, default_encode

logger = logging.getLogger(__name__)

# Pika is verbose.
logging.getLogger("pika").setLevel(logging.WARNING)


def _on_message(
    channel: pika.channel.Channel,
    method: pika.spec.Basic.Deliver,
    properties: pika.BasicProperties,
    body: bytes,
    args: Tuple[List[threading.Thread], Decorator, Decode, ConsumerCallback, State],
) -> None:
    (threads, decorator, decode, callback, state) = args
    thread = threading.Thread(target=decorator, args=(decode, callback, state, channel, method, properties, body))
    thread.start()
    threads.append(thread)


class Base:
    """
    Provides :meth:`~Base.format_routing_key`, which is used by all methods in other mixins that accept routing keys,
    in order to namespace the routing keys.

    Other mixins should list attributes that can - and are expected to - be used safely in consumer callbacks in a
    ``__safe__`` class attribute.
    """

    __safe__ = ["format_routing_key"]

    def __init__(self, *, routing_key_template: str = "{routing_key}", **kwargs: Any):
        """
        :param routing_key_template:
            a `format string <https://docs.python.org/3/library/string.html#format-string-syntax>`__ that must contain
            the ``{routing_key}`` replacement field and that may contain other fields matching writable attributes
        """
        #: The format string for the routing key.
        self.routing_key_template = routing_key_template  # type: str

    def format_routing_key(self, routing_key: str) -> str:
        """
        Format the routing key.

        :param routing_key: the routing key
        :returns: the formatted routing key
        """
        return self.routing_key_template.format(routing_key=routing_key, **self.__dict__)

    @property  # type: ignore # https://github.com/python/mypy/issues/1362
    @functools.lru_cache(maxsize=None)
    def __getsafe__(self) -> Set[str]:
        """
        Attributes that can be used safely in consumer callbacks, across all base classes.
        """
        return {attr for base in type(self).__bases__ for attr in getattr(base, "__safe__", [])} | set(
            type(self).__safe__
        )


class Blocking:
    """
    Uses a blocking connection while avoiding deadlocks due to
    `blocked connections <https://www.rabbitmq.com/connection-blocked.html>`__.
    """

    # The connection isn't "safe to use" but it can be "used safely" like in:
    # https://github.com/pika/pika/blob/master/examples/basic_consumer_threaded.py
    __safe__ = ["connection"]

    def __init__(
        self,
        *,
        url: str = "amqp://127.0.0.1",
        blocked_connection_timeout: float = 1800,
        prefetch_count: int = 1,
        **kwargs: Any
    ):
        """
        Connect to RabbitMQ and create a channel.

        :param url: the connection string (don't set a blocked_connection_timeout query string parameter)
        :param blocked_connection_timeout: the timeout, in seconds, that the connection may remain blocked
        :param prefetch_count: the maximum number of unacknowledged deliveries that are permitted on the channel
        """
        super().__init__(**kwargs)

        parameters = pika.URLParameters(url)
        parameters.blocked_connection_timeout = blocked_connection_timeout

        #: The connection.
        self.connection = pika.BlockingConnection(parameters)  # type: pika.BlockingConnection

        #: The channel.
        self.channel = self.connection.channel()  # type: pika.adapters.blocking_connection.BlockingChannel
        self.channel.basic_qos(prefetch_count=prefetch_count)

    def close(self) -> None:
        """
        Close the connection.
        """
        self.connection.close()


class Publisher:
    """
    An abstract parent class. Use :class:`~yapw.clients.Durable` or :class:`~yapw.clients.Transient` instead.
    """

    durable: bool
    delivery_mode: int

    # Attributes that this mixin expects from base classes.
    format_routing_key: Callable[["Publisher", str], str]
    channel: pika.channel.Channel

    __safe__ = ["exchange", "encode", "content_type", "delivery_mode"]

    def __init__(
        self,
        *,
        exchange: str = "",
        exchange_type: ExchangeType = ExchangeType.direct,
        encode: Encode = default_encode,
        content_type: str = "application/json",
        routing_key_template: str = "{exchange}_{routing_key}",
        **kwargs: Any
    ):
        """
        Declare an exchange, unless using the default exchange.

        When publishing a message, by default, its body is encoded using :func:`yapw.util.default_encode`, and its
        content type is set to "application/json". Use the ``encode`` and ``content_type`` keyword arguments to change
        this. The ``encode`` must be a function that accepts ``(message, content_type)`` arguments and returns bytes.

        :param exchange: the exchange name
        :param exchange_type: the exchange type
        :param encode: the message body's encoder
        :param content_type: the message's content type
        :param routing_key_template: see :meth:`~yapw.clients.Base.__init__`
        """
        super().__init__(routing_key_template=routing_key_template, **kwargs)  # type: ignore # python/mypy#5887

        #: The exchange name.
        self.exchange = exchange  # type: str
        #: The message body's encoder.
        self.encode = encode  # type: Encode
        #: The message's content type.
        self.content_type = content_type  # type: str

        if self.exchange:
            self.channel.exchange_declare(exchange=self.exchange, exchange_type=exchange_type, durable=self.durable)

    def declare_queue(self, queue: str, routing_keys: Optional[List[str]] = None) -> None:
        """
        Declare a queue, and bind it to the exchange with the routing keys. If no routing keys are provided, the queue
        is bound to the exchange using its name as the routing key.

        :param queue: the queue's name
        :param routing_keys: the queue's routing keys
        """
        if not routing_keys:
            routing_keys = [queue]

        formatted = self.format_routing_key(queue)
        self.channel.queue_declare(queue=formatted, durable=self.durable)

        for routing_key in routing_keys:
            routing_key = self.format_routing_key(routing_key)
            self.channel.queue_bind(exchange=self.exchange, queue=formatted, routing_key=routing_key)

    def publish(self, message: Any, routing_key: str) -> None:
        """
        Publish from the main thread, with the provided message and routing key, and with the configured exchange.

        :param message: a decoded message
        :param routing_key: the routing key
        """
        keywords = basic_publish_kwargs(self, message, routing_key)

        self.channel.basic_publish(**keywords)
        logger.debug(*basic_publish_debug_args(self.channel, message, keywords))


class Transient(Publisher):
    """
    Declares a transient exchange, declares transient queues, and uses transient messages.
    """

    durable = False
    delivery_mode = 1


class Durable(Publisher):
    """
    Declares a durable exchange, declares durable queues, and uses persistent messages.
    """

    durable = True
    delivery_mode = 2


# https://github.com/pika/pika/blob/master/examples/basic_consumer_threaded.py
class Threaded:
    """
    Runs the consumer callback in separate threads.
    """

    # Attributes that this mixin expects from base classes.
    format_routing_key: Callable[["Threaded", str], str]
    declare_queue: Callable[["Threaded", str, Optional[List[str]]], None]
    connection: pika.BlockingConnection
    channel: pika.adapters.blocking_connection.BlockingChannel

    def __init__(self, decode: Decode = default_decode, **kwargs: Any):
        """
        Install signal handlers to stop consuming messages, wait for threads to terminate, and close the connection.

        When consuming a message, by default, its body is decoded using :func:`yapw.decorators.default_decode`. Use the
        ``decode`` keyword argument to change this. The ``decode`` must be a function that accepts ``(state, channel,
        method, properties, body)`` arguments (like the consumer callback) and returns a decoded message.

        :param decode: the message body's decoder
        """
        super().__init__(**kwargs)

        #: The message body's decoder.
        self.decode = decode  # type: Decode

        install_signal_handlers(self._on_shutdown)

    def consume(
        self,
        callback: ConsumerCallback,
        queue: str,
        routing_keys: Optional[List[str]] = None,
        decorator: Decorator = halt,
    ) -> None:
        """
        Declare a queue, bind it to the exchange with the routing keys, and start consuming messages from that queue.
        If no routing keys are provided, the queue is bound to the exchange using its name as the routing key.

        The consumer callback must be a function that accepts ``(state, channel, method, properties, body)`` arguments,
        all but the first of which are the same as Pika's ``basic_consume``. The ``state`` argument is needed to pass
        attributes to :mod:`yapw.methods.blocking` functions.

        :param callback: the consumer callback
        :param queue: the queue's name
        :param routing_keys: the queue's routing keys
        :param decorator: the decorator of the consumer callback
        """
        self.declare_queue(queue, routing_keys)
        formatted = self.format_routing_key(queue)

        # Don't pass `self` to the callback, to prevent use of unsafe attributes and mutation of safe attributes.
        klass = namedtuple("State", self.__getsafe__)  # type: ignore # https://github.com/python/mypy/issues/848
        state = klass(**{attr: getattr(self, attr) for attr in self.__getsafe__})  # type: ignore

        threads = []  # type: List[threading.Thread]
        on_message_callback = functools.partial(_on_message, args=(threads, decorator, self.decode, callback, state))
        self.channel.basic_consume(formatted, on_message_callback)

        logger.debug("Consuming messages on channel %s from queue %s", self.channel.channel_number, formatted)
        try:
            self.channel.start_consuming()
        finally:
            for thread in threads:
                thread.join()
            self.connection.close()

    def _on_shutdown(self, signum: int, frame: Optional[FrameType]) -> None:
        install_signal_handlers(signal.SIG_IGN)
        logger.info("Received %s, shutting down gracefully", signal_names[signum])
        self.channel.stop_consuming()
