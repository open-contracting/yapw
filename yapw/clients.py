"""
Classes for RabbitMQ clients.

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
from concurrent.futures import ThreadPoolExecutor
from types import FrameType
from typing import Any, Dict, List, Optional, Set, Tuple, Union

import pika
import pika.exceptions
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
    args: Tuple[ThreadPoolExecutor, Decorator, Decode, ConsumerCallback, State],
) -> None:
    (executor, decorator, decode, callback, state) = args
    executor.submit(decorator, decode, callback, state, channel, method, properties, body)


class Base:
    """
    Provides :meth:`~Base.format_routing_key`, which is used by all methods in other classes that accept routing keys,
    in order to namespace the routing keys.
    """

    connection: Union[pika.BlockingConnection, pika.SelectConnection]

    # The connection isn't "safe to use" but it can be "used safely" like in:
    # https://github.com/pika/pika/blob/master/examples/basic_consumer_threaded.py
    #: Attributes that can - and are expected to - be used safely in consumer callbacks.
    __safe__ = ["connection", "exchange", "encode", "content_type", "delivery_mode", "format_routing_key"]

    def __init__(
        self,
        *,
        url: str = "amqp://127.0.0.1",
        blocked_connection_timeout: float = 1800,
        durable: bool = True,
        exchange: str = "",
        exchange_type: ExchangeType = ExchangeType.direct,
        prefetch_count: int = 1,
        decode: Decode = default_decode,
        encode: Encode = default_encode,
        content_type: str = "application/json",
        routing_key_template: str = "{exchange}_{routing_key}",
        **kwargs: Any,
    ):
        """
        When consuming a message, by default, its body is decoded using :func:`yapw.decorators.default_decode`. Use the
        ``decode`` keyword argument to change this. The ``decode`` must be a function that accepts ``(state, channel,
        method, properties, body)`` arguments (like the consumer callback) and returns a decoded message.

        When publishing a message, by default, its body is encoded using :func:`yapw.util.default_encode`, and its
        content type is set to "application/json". Use the ``encode`` and ``content_type`` keyword arguments to change
        this. The ``encode`` must be a function that accepts ``(message, content_type)`` arguments and returns bytes.

        :param url: the connection string (don't set a blocked_connection_timeout query string parameter)
        :param blocked_connection_timeout: the timeout, in seconds, that the connection may remain blocked
        :param durable: whether to declare a durable exchange, declare durable queues, and publish persistent messages
        :param exchange: the exchange name
        :param exchange_type: the exchange type
        :param prefetch_count: the maximum number of unacknowledged deliveries that are permitted on the channel
        :param decode: the message body's decoder
        :param encode: the message bodies' encoder
        :param content_type: the messages' content type
        :param routing_key_template:
            a `format string <https://docs.python.org/3/library/string.html#format-string-syntax>`__ that must contain
            the ``{routing_key}`` replacement field and that may contain other fields matching writable attributes
        """
        #: The RabbitMQ connection parameters.
        self.parameters: pika.URLParameters = pika.URLParameters(url)
        self.parameters.blocked_connection_timeout = blocked_connection_timeout
        #: Whether to declare a durable exchange, declare durable queues, and publish persistent messages.
        self.durable: bool = durable
        #: The exchange name.
        self.exchange: str = exchange
        #: The exchange type.
        self.exchange_type: ExchangeType = exchange_type
        #: The maximum number of unacknowledged messages per consumer.
        self.prefetch_count: int = prefetch_count
        #: The message bodies' decoder.
        self.decode: Decode = decode
        #: The message bodies' encoder.
        self.encode: Encode = encode
        #: The messages' content type.
        self.content_type: str = content_type
        #: The messages' delivery mode.
        self.delivery_mode = 2 if durable else 1
        #: The format string for the routing key.
        self.routing_key_template: str = routing_key_template

    def declare_exchange_and_configure_prefetch(
        self, channel: Union[pika.channel.Channel, pika.adapters.blocking_connection.BlockingChannel]
    ) -> None:
        """
        Declare an exchange, unless using the default exchange, and set the prefetch count.
        """
        channel.basic_qos(prefetch_count=self.prefetch_count)
        if self.exchange:
            channel.exchange_declare(exchange=self.exchange, exchange_type=self.exchange_type, durable=self.durable)

    def declare_queue(
        self,
        channel: Union[pika.channel.Channel, pika.adapters.blocking_connection.BlockingChannel],
        queue: str,
        routing_keys: Optional[List[str]] = None,
        arguments: Optional[Dict[str, str]] = None,
    ) -> None:
        """
        Declare a queue, and bind it to the exchange with the routing keys. If no routing keys are provided, the queue
        is bound to the exchange using its name as the routing key.

        :param channel: the current channel
        :param queue: the queue's name
        :param routing_keys: the queue's routing keys
        :param arguments: any custom key-value arguments
        """
        if not routing_keys:
            routing_keys = [queue]

        formatted = self.format_routing_key(queue)
        channel.queue_declare(queue=formatted, durable=self.durable, arguments=arguments)

        for routing_key in routing_keys:
            routing_key = self.format_routing_key(routing_key)
            channel.queue_bind(exchange=self.exchange, queue=formatted, routing_key=routing_key)

    def publish(
        self,
        channel: Union[pika.channel.Channel, pika.adapters.blocking_connection.BlockingChannel],
        message: Any,
        routing_key: str,
    ) -> None:
        """
        Publish from the main thread, with the provided message and routing key, and with the configured exchange.

        :param channel: the current channel
        :param message: a decoded message
        :param routing_key: the routing key
        """
        keywords = basic_publish_kwargs(self, message, routing_key)

        channel.basic_publish(**keywords)
        logger.debug(*basic_publish_debug_args(channel, message, keywords))

    def format_routing_key(self, routing_key: str) -> str:
        """
        Format the routing key.

        :param routing_key: the routing key
        :returns: the formatted routing key
        """
        return self.routing_key_template.format(routing_key=routing_key, **self.__dict__)

    def close(self) -> None:
        """
        Close the connection.
        """
        self.connection.close()

    @property
    @functools.lru_cache(maxsize=None)
    def __getsafe__(self) -> Set[str]:
        """
        Attributes that can be used safely in consumer callbacks, across all base classes.
        """
        return {attr for base in type(self).__bases__ for attr in getattr(base, "__safe__", [])} | set(
            type(self).__safe__
        )


class Blocking(Base):
    """
    Uses a `blocking connection adapter <https://pika.readthedocs.io/en/stable/modules/adapters/blocking.html>`__ while
    avoiding deadlocks due to `blocked connections <https://www.rabbitmq.com/connection-blocked.html>`__.
    """

    def __init__(self, **kwargs: Any):
        """
        Connect to RabbitMQ, create a channel and declare an exchange, unless using the default exchange.
        """
        super().__init__(**kwargs)

        #: The connection.
        self.connection: pika.BlockingConnection = pika.BlockingConnection(self.parameters)

        #: The channel.
        self.channel: pika.adapters.blocking_connection.BlockingChannel = self.connection.channel()
        self.declare_exchange_and_configure_prefetch(self.channel)

    def declare_queue(self, *args: Any, **kwargs: Any) -> None:
        """
        Declare a queue, and bind it to the exchange with the routing keys. If no routing keys are provided, the queue
        is bound to the exchange using its name as the routing key.
        """
        super().declare_queue(self.channel, *args, **kwargs)

    def publish(self, *args: Any, **kwargs: Any) -> None:
        """
        Publish from the main thread, with the provided message and routing key, and with the configured exchange.
        """
        super().publish(self.channel, *args, **kwargs)

    # https://github.com/pika/pika/blob/master/examples/basic_consumer_threaded.py
    def consume(
        self,
        callback: ConsumerCallback,
        queue: str,
        routing_keys: Optional[List[str]] = None,
        decorator: Decorator = halt,
        arguments: Optional[Dict[str, str]] = None,
    ) -> None:
        """
        Declare a queue, bind it to the exchange with the routing keys, and start consuming messages from that queue.
        If no routing keys are provided, the queue is bound to the exchange using its name as the routing key.

        Run the consumer callback in separate threads. If the client was instantiated in the main thread, install
        signal handlers to stop consuming messages, wait for threads to terminate, and close the connection.

        The consumer callback must be a function that accepts ``(state, channel, method, properties, body)`` arguments,
        all but the first of which are the same as Pika's ``basic_consume``. The ``state`` argument is needed to pass
        attributes to :mod:`yapw.methods.blocking` functions.

        :param callback: the consumer callback
        :param queue: the queue's name
        :param routing_keys: the queue's routing keys
        :param decorator: the decorator of the consumer callback
        :param arguments: the ``arguments`` parameter to the ``queue_declare`` method
        """
        self.declare_queue(queue, routing_keys, arguments)
        formatted = self.format_routing_key(queue)

        # Don't pass `self` to the callback, to prevent use of unsafe attributes and mutation of safe attributes.
        klass = namedtuple("State", self.__getsafe__)  # type: ignore # python/mypy#848 "This just never will happen"
        state = klass(**{attr: getattr(self, attr) for attr in self.__getsafe__})  # type: ignore

        if threading.current_thread() is threading.main_thread():
            install_signal_handlers(self._on_shutdown)

        executor = ThreadPoolExecutor(thread_name_prefix=f"yapw-{queue}")
        on_message_callback = functools.partial(_on_message, args=(executor, decorator, self.decode, callback, state))
        self.channel.basic_consume(formatted, on_message_callback)

        logger.debug("Consuming messages on channel %s from queue %s", self.channel.channel_number, formatted)
        try:
            self.channel.start_consuming()
        except pika.exceptions.ConnectionClosedByBroker:
            executor.shutdown()
            # The connection is already closed.
        else:
            executor.shutdown()
            self.connection.close()

    def _on_shutdown(self, signum: int, frame: Optional[FrameType]) -> None:
        install_signal_handlers(signal.SIG_IGN)
        logger.info("Received %s, shutting down gracefully", signal_names[signum])
        self.channel.stop_consuming()
