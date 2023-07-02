"""
Classes for RabbitMQ clients.

.. warning::

   Importing this module sets the level of the "pika" logger to ``WARNING``, so that consumers can use the ``DEBUG``
   and ``INFO`` levels without their messages getting lost in Pika's verbosity.
"""
from __future__ import annotations

import functools
import logging
import signal
from collections import namedtuple
from concurrent.futures import ThreadPoolExecutor
from types import FrameType
from typing import TYPE_CHECKING, Any, Generic, TypeVar

import pika
import pika.exceptions
from pika.exchange_type import ExchangeType

from yapw.decorators import halt
from yapw.ossignal import install_signal_handlers, signal_names
from yapw.types import ConsumerCallback, Decode, Decorator, Encode, State
from yapw.util import basic_publish_debug_args, basic_publish_kwargs, default_decode, default_encode

T = TypeVar("T")
logger = logging.getLogger(__name__)

# Pika is verbose.
logging.getLogger("pika").setLevel(logging.WARNING)


def _on_message(
    channel: pika.channel.Channel,
    method: pika.spec.Basic.Deliver,
    properties: pika.BasicProperties,
    body: bytes,
    args: tuple[ThreadPoolExecutor, Decorator, Decode, ConsumerCallback, State[Any]],
) -> None:
    (executor, decorator, decode, callback, state) = args
    executor.submit(decorator, decode, callback, state, channel, method, properties, body)


class Base(Generic[T]):
    """
    Base class providing common functionality to other clients. You cannot use this class directly.

    When consuming a message, by default, its body is decoded using :func:`yapw.util.default_decode`. Use the
    ``decode`` keyword argument to change this. The ``decode`` must be a function that accepts ``(state, channel,
    method, properties, body)`` arguments (like the consumer callback) and returns a decoded message.

    When publishing a message, by default, its body is encoded using :func:`yapw.util.default_encode`, and its content
    type is set to "application/json". Use the ``encode`` and ``content_type`` keyword arguments to change this. The
    ``encode`` must be a function that accepts ``(message, content_type)`` arguments and returns bytes.

    :meth:`~Base.format_routing_key` must be used by methods in subclasses that accept routing keys, in order to
    namespace the routing keys.
    """

    #: The connection.
    connection: T
    #: The channel.
    channel: pika.channel.Channel | pika.adapters.blocking_connection.BlockingChannel

    # `connection` and `interrupt` aren't "safe to use" but can be "used safely" like in:
    # https://github.com/pika/pika/blob/master/examples/basic_consumer_threaded.py
    #: Attributes that can - and are expected to - be used safely in consumer callbacks.
    __safe__ = ["connection", "interrupt", "exchange", "encode", "content_type", "delivery_mode", "format_routing_key"]

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
    ):
        """
        :param url: the connection string (don't set a ``blocked_connection_timeout`` query string parameter)
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
        self.parameters = pika.URLParameters(url)
        # https://pika.readthedocs.io/en/stable/examples/heartbeat_and_blocked_timeouts.html
        self.parameters.blocked_connection_timeout = blocked_connection_timeout
        #: Whether to declare a durable exchange, declare durable queues, and publish persistent messages.
        self.durable = durable
        #: The exchange name.
        self.exchange = exchange
        #: The exchange type.
        self.exchange_type = exchange_type
        #: The maximum number of unacknowledged messages per consumer.
        self.prefetch_count = prefetch_count
        #: The message bodies' decoder.
        self.decode = decode
        #: The message bodies' encoder.
        self.encode = encode
        #: The messages' content type.
        self.content_type = content_type
        #: The format string for the routing key.
        self.routing_key_template = routing_key_template

        #: The messages' delivery mode.
        self.delivery_mode = 2 if self.durable else 1
        #: The consumer's tag.
        self.consumer_tag = ""
        #: The thread pool executor.
        self.executor = ThreadPoolExecutor(thread_name_prefix=f"yapw-{self.exchange}")

    def format_routing_key(self, routing_key: str) -> str:
        """
        Namespace the routing key.

        :param routing_key: the routing key
        :returns: the formatted routing key
        """
        return self.routing_key_template.format(routing_key=routing_key, **self.__dict__)

    def publish(
        self,
        message: Any,
        routing_key: str,
    ) -> None:
        """
        Publish the ``message`` with the ``routing_key`` to the configured exchange, from the IO loop thread.

        :param message: a decoded message
        :param routing_key: the routing key
        """
        keywords = basic_publish_kwargs(self, message, routing_key)

        self.channel.basic_publish(**keywords)
        logger.debug(*basic_publish_debug_args(self.channel, message, keywords))

    def start_consumer(self, callback: ConsumerCallback, decorator: Decorator, queue_name: str) -> None:
        """
        Start consuming messages from the queue.

        Run the consumer callback in separate threads, to not block the IO loop.

        The consumer callback is a function that accepts ``(state, channel, method, properties, body)`` arguments. The
        ``state`` argument contains thread-safe attributes. The rest are the same as Pika's ``basic_consume``.
        """
        self.channel.add_on_cancel_callback(self.channel_cancel)

        cb = functools.partial(_on_message, args=(self.executor, decorator, self.decode, callback, self.state))

        self.consumer_tag = self.channel.basic_consume(queue_name, cb)
        logger.debug("Consuming messages on channel %s from queue %s", self.channel.channel_number, queue_name)

    # https://www.rabbitmq.com/consumer-cancel.html
    def channel_cancel(self, method: Any) -> Any:  # different method types for each channel class
        """
        Close the channel. (RabbitMQ uses ``basic.cancel`` if a channel is consuming a queue and the queue is deleted.)
        """
        logger.error("Consumer was cancelled by broker, stopping: %r", method)
        if hasattr(self.channel, "stop_consuming"):
            self.channel.stop_consuming()
        else:
            if TYPE_CHECKING:  # can't use TypeGuard, as first branch uses channel and this branch uses connection
                assert isinstance(self.connection, pika.SelectConnection)
            # Keep channel open until threads terminate. Ensure the channel closes after any thread-safe callbacks.
            self.executor.shutdown(cancel_futures=True)
            self.connection.ioloop.call_later(0, self.channel.close)

    def _on_signal(self, signum: int, frame: FrameType | None) -> None:
        install_signal_handlers(signal.SIG_IGN)
        logger.info("Received %s, shutting down gracefully", signal_names[signum])
        self.interrupt()

    def interrupt(self) -> None:
        """
        Override this method in subclasses to shut down gracefully (e.g. wait for threads to terminate).
        """
        pass

    @property
    def state(self):  # type: ignore # anonymous class
        """
        A named tuple of attributes that can be used within threads.
        """
        # Don't pass `self` to the callback, to prevent use of unsafe attributes and mutation of safe attributes.
        klass = namedtuple("State", self.__getsafe__)  # type: ignore # python/mypy#848 "This just never will happen"
        return klass(**{attr: getattr(self, attr) for attr in self.__getsafe__})

    @property
    @functools.cache
    def __getsafe__(self) -> set[str]:
        """
        The attributes that can be used safely in consumer callbacks, across all base classes.
        """
        return {attr for base in type(self).__bases__ for attr in getattr(base, "__safe__", [])} | set(
            type(self).__safe__
        )


class Blocking(Base[pika.BlockingConnection]):
    """
    Uses Pika's `BlockingConnection adapter <https://pika.readthedocs.io/en/stable/modules/adapters/blocking.html>`__.
    """

    def __init__(self, **kwargs: Any):
        """
        Connect to RabbitMQ, create a channel, set the prefetch count, and declare an exchange, unless using the
        default exchange.
        """
        super().__init__(**kwargs)

        #: The connection.
        self.connection = pika.BlockingConnection(self.parameters)

        #: The channel.
        self.channel: pika.adapters.blocking_connection.BlockingChannel = self.connection.channel()
        self.channel.basic_qos(prefetch_count=self.prefetch_count)

        if self.exchange:
            self.channel.exchange_declare(
                exchange=self.exchange, exchange_type=self.exchange_type, durable=self.durable
            )

    def declare_queue(
        self, queue: str, routing_keys: list[str] | None = None, arguments: dict[str, str] | None = None
    ) -> None:
        """
        Declare a queue, and bind it to the exchange with the routing keys. If no routing keys are provided, the queue
        is bound to the exchange using its name as the routing key.

        :param queue: the queue's name
        :param routing_keys: the queue's routing keys
        :param arguments: any custom key-value arguments
        """
        if not routing_keys:
            routing_keys = [queue]

        queue_name = self.format_routing_key(queue)
        self.channel.queue_declare(queue=queue_name, durable=self.durable, arguments=arguments)

        for routing_key in routing_keys:
            routing_key = self.format_routing_key(routing_key)
            self.channel.queue_bind(queue=queue_name, exchange=self.exchange, routing_key=routing_key)

    # https://github.com/pika/pika/blob/master/examples/basic_consumer_threaded.py
    def consume(
        self,
        callback: ConsumerCallback,
        queue: str,
        routing_keys: list[str] | None = None,
        decorator: Decorator = halt,
        arguments: dict[str, str] | None = None,
    ) -> None:
        """
        Declare a queue, bind it to the exchange with the routing keys, and start consuming messages from that queue.

        If no routing keys are provided, the queue is bound to the exchange using its name as the routing key.

        Install signal handlers to wait for threads to terminate.

        .. seealso::

           :meth:`yapw.clients.Base.start_consumer`

        :param callback: the consumer callback
        :param queue: the queue's name
        :param routing_keys: the queue's routing keys
        :param decorator: the decorator of the consumer callback
        :param arguments: the ``arguments`` parameter to the ``queue_declare`` method
        """
        self.declare_queue(queue, routing_keys, arguments)
        queue_name = self.format_routing_key(queue)

        self.start_consumer(callback, decorator, queue_name)

        # The callback calls stop_consuming(), so install it after start_consumer() sets consumer_tag.
        install_signal_handlers(self._on_signal)

        try:
            self.channel.start_consuming()
        finally:
            # Keep channel open until threads terminate.
            self.executor.shutdown(cancel_futures=True)
            self.connection.close()

    def close(self) -> None:
        """
        Close the connection: for example, after sending messages from a simple publisher.
        """
        self.connection.close()

    def interrupt(self) -> None:
        """
        Cancel the consumer, which causes the threads to terminate and the connection to close.
        """
        self.channel.stop_consuming(self.consumer_tag)


# https://github.com/pika/pika/blob/main/examples/asynchronous_consumer_example.py
# https://github.com/pika/pika/blob/main/docs/examples/tornado_consumer.rst
# https://github.com/pika/pika/issues/727#issuecomment-213644075
class Async(Base[pika.SelectConnection]):
    """
    Uses Pika's `SelectConnection adapter <https://pika.readthedocs.io/en/stable/modules/adapters/select.html>`__.
    Reconnects to RabbitMQ if the connection is closed unexpectedly or can't be established.

    By calling :meth:`~yapw.clients.Async.start`, this client connects to RabbitMQ, installs signal handlers, and
    starts the IO loop. Once the IO loop starts, it creates a channel, sets the prefetch count, and declares the
    exchange (unless using the default exchange).

    One the exchange is declared, the callback calls :meth:`~yapw.clients.Async.exchange_ready`. You must define this
    method in a subclass, to do any work you need.

    The signal handlers cancel the consumer, if consuming and if the channel is open. Otherwise, they wait for threads
    to terminate and close the connection. This graceful shutdown also occurs if the broker cancels the consumer or if
    the channel closes for any another reason.

    If the connection becomes `blocked <https://www.rabbitmq.com/connection-blocked.html>`__ or unblocked, the
    ``blocked`` attribute is set to ``True`` or ``False``, respectively. Your code can use this attribute to, for
    example, pause, buffer or reschedule deliveries.
    """

    # RabbitMQ takes about 10 seconds to restart.
    RECONNECT_DELAY = 15

    def __init__(self, **kwargs: Any):
        """
        Initialize the client's state.
        """
        super().__init__(**kwargs)

        #: Whether the connection is `blocked <https://www.rabbitmq.com/connection-blocked.html>`__.
        self.blocked = False
        #: Whether the client is being stopped deliberately.
        self.stopping = False

    def start(self) -> None:
        """
        :meth:`Connect<yapw.clients.Async.connect>` to RabbitMQ and start the IO loop.

        Install signal handlers to stop the IO loop correctly.
        """
        self.connect()
        install_signal_handlers(self._on_signal)
        self.connection.ioloop.start()

    def connect(self) -> None:
        """
        Connect to RabbitMQ, create a channel, set the prefetch count, and declare an exchange, unless using the
        default exchange.
        """
        self.connection = pika.SelectConnection(
            self.parameters,
            on_open_callback=self.connection_open_callback,
            on_open_error_callback=self.connection_open_error_callback,
            on_close_callback=self.connection_close_callback,
        )
        self.connection.add_on_connection_blocked_callback(self.connection_blocked_callback)
        self.connection.add_on_connection_unblocked_callback(self.connection_unblocked_callback)

    def connection_blocked_callback(self, connection: pika.connection.Connection, method: Any) -> None:
        """
        Mark the client as blocked.

        Subclasses must implement any logic for pausing deliveries or filling buffers.
        """
        logger.warning("Connection blocked")
        self.blocked = True

    def connection_unblocked_callback(self, connection: pika.connection.Connection, method: Any) -> None:
        """
        Mark the client as unblocked.

        Subclasses must implement any logic for resuming deliveries or clearing buffers.
        """
        logger.warning("Connection unblocked")
        self.blocked = False

    def reconnect(self) -> None:
        """
        Reconnect to RabbitMQ, unless a signal was received while the timer was running. If so, stop the IO loop.
        """
        if self.stopping:
            self.connection.ioloop.stop()
        else:
            # Reset mutable attributes.
            self.blocked = False
            self.consumer_tag = ""
            self.executor = ThreadPoolExecutor(thread_name_prefix=f"yapw-{self.exchange}")
            self.connect()

    def connection_open_error_callback(self, connection: pika.connection.Connection, error: Exception | str) -> None:
        """Retry, once the connection couldn't be established."""
        logger.error("Connection failed, retrying in %ds: %s", self.RECONNECT_DELAY, error)
        self.connection.ioloop.call_later(self.RECONNECT_DELAY, self.reconnect)

    def connection_close_callback(self, connection: pika.connection.Connection, reason: Exception) -> None:
        """Reconnect, if the connection was closed unexpectedly. Otherwise, stop the IO loop."""
        if self.stopping:
            self.connection.ioloop.stop()
        else:
            logger.error("Connection closed, reconnecting in %ds: %s", self.RECONNECT_DELAY, reason)
            self.connection.ioloop.call_later(self.RECONNECT_DELAY, self.reconnect)

    def interrupt(self) -> None:
        """
        `Cancel <https://www.rabbitmq.com/consumers.html#unsubscribing>`__ the consumer if consuming and if the channel
        is open. Otherwise, wait for threads to terminate and close the connection.
        """
        # Signals handlers are installed, so the IO loop is not stopped. If there were no signal handlers, the IO
        # loop would have been stopped, and would need to restart to send buffered requests to RabbitMQ.

        # Change the client's state to stopping, to prevent infinite reconnection.
        self.stopping = True

        if self.consumer_tag and not self.channel.is_closed and not self.channel.is_closing:
            self.channel.basic_cancel(self.consumer_tag, self.channel_cancelok_callback)
        elif not self.connection.is_closed and not self.connection.is_closing:
            # The channel is already closed. Free any resources without waiting for threads.
            self.executor.shutdown(wait=False, cancel_futures=True)
            self.connection.close()

    def connection_open_callback(self, connection: pika.connection.Connection) -> None:
        """Open a channel, once the connection is open."""
        connection.channel(on_open_callback=self.channel_open_callback)

    def channel_open_callback(self, channel: pika.channel.Channel) -> None:
        """Set the prefetch count, once the channel is open."""
        self.channel: pika.channel.Channel = channel
        self.channel.add_on_close_callback(self.channel_close_callback)
        channel.basic_qos(prefetch_count=self.prefetch_count, callback=self.channel_qosok_callback)

    def channel_cancelok_callback(self, method: pika.frame.Method[pika.spec.Basic.CancelOk]) -> Any:
        """
        Close the channel, once the consumer is cancelled. The :meth:`~yapw.clients.Async.channel_close_callback`
        closes the connection.
        """
        # Keep channel open until threads terminate. Ensure the channel closes after any thread-safe callbacks.
        self.executor.shutdown(cancel_futures=True)
        self.connection.ioloop.call_later(0, self.channel.close)

    def channel_close_callback(self, channel: pika.channel.Channel, reason: Exception) -> None:
        """
        Close the connection, once the client cancelled the consumer or once RabbitMQ closed the channel due to, e.g.,
        redeclaring exchanges with inconsistent parameters. Log a warning, in case it was the latter.
        """
        logger.warning("Channel %i was closed: %s", channel, reason)
        # pika's connection.close() closes all channels. It can update the connection state before this callback runs.
        if not self.connection.is_closed and not self.connection.is_closing:
            # The channel is already closed. Free any resources without waiting for threads.
            self.executor.shutdown(wait=False, cancel_futures=True)
            self.connection.close()

    def channel_qosok_callback(self, method: pika.frame.Method[pika.spec.Basic.QosOk]) -> None:
        """Declare the exchange, once the prefetch count is set."""
        if self.exchange:
            logger.debug(
                "Declaring %s %s exchange %s",
                "durable" if self.durable else "transient",
                ExchangeType[self.exchange_type].value,  # type: ignore # "Enum index should be a string"
                self.exchange,
            )
            self.channel.exchange_declare(
                exchange=self.exchange,
                exchange_type=self.exchange_type,
                durable=self.durable,
                callback=self.exchange_declareok_callback,
            )
        else:
            self.exchange_ready()

    def exchange_declareok_callback(self, method: pika.frame.Method[pika.spec.Exchange.DeclareOk]) -> None:
        """Perform user-specified actions, once the exchange is declared."""
        self.exchange_ready()

    def exchange_ready(self) -> None:
        """Override this method in subclasses."""
        raise NotImplementedError


class AsyncConsumer(Async):
    """
    An asynchronous consumer, extending :class:`~yapw.clients.Async`.

    After calling :meth:`~yapw.clients.Async.start`, this client declares the ``queue``, binds it to the exchange with
    the ``routing_keys``, and starts consuming messages from that queue (see :meth:`yapw.clients.Base.start_consumer`),
    using the consumer ``callback`` and its ``decorator``.

    If no ``routing_keys`` are provided, the ``queue`` is bound to the exchange using its name as the routing key.

    The ``callback`` and ``queue`` keyword arguments are required.
    """

    def __init__(
        self,
        *,
        callback: ConsumerCallback,
        queue: str,
        routing_keys: list[str] | None = None,
        decorator: Decorator = halt,
        arguments: dict[str, str] | None = None,
        **kwargs: Any,
    ) -> None:
        """
        :param callback: the consumer callback
        :param queue: the queue's name
        :param routing_keys: the queue's routing keys
        :param decorator: the decorator of the consumer callback
        :param arguments: the ``arguments`` parameter to the ``queue_declare`` method
        """
        super().__init__(**kwargs)

        #: The queue's name.
        self.queue = queue
        #: The queue's routing keys.
        self.routing_keys = routing_keys or [queue]
        #: The ``arguments`` parameter to the ``queue_declare`` method.
        self.arguments = arguments
        #: The consumer callback.
        self.callback = callback
        #: The decorator of the consumer callback.
        self.decorator = decorator

    def exchange_ready(self) -> None:
        """Declare the queue, once the exchange is declared."""
        queue_name = self.format_routing_key(self.queue)
        cb = functools.partial(self.queue_declareok_callback, queue_name=queue_name)
        self.channel.queue_declare(queue=queue_name, durable=self.durable, arguments=self.arguments, callback=cb)

    def queue_declareok_callback(self, method: pika.frame.Method[pika.spec.Queue.DeclareOk], queue_name: str) -> None:
        """Bind the queue to the first routing key, once the queue is declared."""
        self._bind_queue(queue_name, 0)

    def queue_bindok_callback(
        self, method: pika.frame.Method[pika.spec.Queue.BindOk], queue_name: str, index: int
    ) -> None:
        """Bind the queue to the remaining routing keys, or start consuming if all routing keys bound."""
        if index < len(self.routing_keys):
            self._bind_queue(queue_name, index)
        else:
            self.start_consumer(self.callback, self.decorator, queue_name)

    def _bind_queue(self, queue_name: str, index: int) -> None:
        routing_key = self.format_routing_key(self.routing_keys[index])
        cb = functools.partial(self.queue_bindok_callback, queue_name=queue_name, index=index + 1)
        self.channel.queue_bind(queue=queue_name, exchange=self.exchange, routing_key=routing_key, callback=cb)
