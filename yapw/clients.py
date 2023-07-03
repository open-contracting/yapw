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
import threading
from collections import namedtuple
from collections.abc import Callable
from concurrent.futures import ThreadPoolExecutor
from types import FrameType
from typing import Any, Generic, TypeVar

import pika
from pika.adapters.asyncio_connection import AsyncioConnection
from pika.exceptions import ProbableAccessDeniedError, ProbableAuthenticationError
from pika.exchange_type import ExchangeType

from yapw.decorators import halt
from yapw.ossignal import signal_names
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
    args: tuple[Callable[..., None], Decorator, Decode, ConsumerCallback, State[Any]],
) -> None:
    (submit, decorator, decode, callback, state) = args
    submit(decorator, decode, callback, state, channel, method, properties, body)


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

    # Since Python 3.11, asyncio handles SIGINT, to avoid internals being interrupted.
    # https://docs.python.org/3/library/asyncio-runner.html#handling-keyboard-interruption
    #
    # Also, if SIGINT were to reach asyncio, the IO loop would stop and new callbacks (like basic_cancel) wouldn't run.
    # To send requests that were buffered by Pika to RabbitMQ, we would need to restart the IO loop.
    # https://docs.python.org/3/library/asyncio-eventloop.html#asyncio.loop.run_forever
    #
    # By adding our own handlers, SIGINT never reaches asyncio.
    # https://docs.python.org/3/library/asyncio-eventloop.html#asyncio.loop.add_signal_handler
    def add_signal_handlers(self, handler: Callable[..., object]) -> None:
        """
        Add handlers for the SIGTERM and SIGINT signals, if the current thread is the main thread.
        """
        if threading.current_thread() is threading.main_thread():
            self.add_signal_handler(signal.SIGTERM, handler)
            self.add_signal_handler(signal.SIGINT, handler)

    def add_signal_handler(self, signalnum: int, handler: Callable[..., object]) -> None:
        """
        Override this method in subclasses to add a handler for a signal (e.g. using :func:`signal.signal` or
        :meth:`asyncio.loop.add_signal_handler`). The handler should remove signal handlers (in order to ignore
        duplicate signals), log a message with a level of ``INFO``, and call :meth:`yapw.clients.base.interrupt`.
        """
        raise NotImplementedError

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
    Uses Pika's :class:`BlockingConnection adapter<pika.adapters.blocking_connection.BlockingConnection>`.
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
        on_message_callback: ConsumerCallback,
        queue: str,
        routing_keys: list[str] | None = None,
        decorator: Decorator = halt,
        arguments: dict[str, str] | None = None,
    ) -> None:
        """
        Declare a queue, bind it to the exchange with the routing keys, and start consuming messages from that queue.

        If no ``routing_keys`` are provided, the ``queue`` is bound to the exchange using its name as the routing key.

        Run the consumer callback in separate threads, to not block the IO loop. Add signal handlers to wait for
        threads to terminate.

        The consumer callback is a function that accepts ``(state, channel, method, properties, body)`` arguments. The
        ``state`` argument contains thread-safe attributes. The rest of the arguments are the same as
        :meth:`pika.channel.Channel.basic_consume`'s ``on_message_callback``.

        :param on_message_callback: the consumer callback
        :param queue: the queue's name
        :param routing_keys: the queue's routing keys
        :param decorator: the decorator of the consumer callback
        :param arguments: the ``arguments`` parameter to the ``queue_declare`` method
        """
        self.declare_queue(queue, routing_keys, arguments)
        queue_name = self.format_routing_key(queue)

        self.channel.add_on_cancel_callback(self.channel_cancel_callback)

        self.executor = ThreadPoolExecutor(thread_name_prefix=f"yapw-{queue}")
        cb = functools.partial(
            _on_message, args=(self.executor.submit, decorator, self.decode, on_message_callback, self.state)
        )

        self.consumer_tag = self.channel.basic_consume(queue_name, cb)
        logger.debug("Consuming messages on channel %s from queue %s", self.channel.channel_number, queue_name)

        # The signal callback calls channel.stop_consuming(), so add handlers after setting consumer_tag.
        self.add_signal_handlers(self._on_signal_callback)

        try:
            self.channel.start_consuming()
        finally:
            # Keep channel open until threads terminate.
            self.executor.shutdown(cancel_futures=True)
            self.connection.close()

    def channel_cancel_callback(self, method: pika.spec.Basic.Cancel) -> Any:
        """
        Cancel the consumer, which causes the threads to terminate and the connection to close.

        RabbitMQ uses `basic.cancel <https://www.rabbitmq.com/consumer-cancel.html>`__ if a channel is consuming a
        queue and the queue is deleted.
        """
        logger.error("Consumer was cancelled by broker, stopping: %r", method)
        self.channel.stop_consuming(self.consumer_tag)

    def add_signal_handler(self, signalnum: int, handler: Callable[..., object]) -> None:
        """
        Add a handler for a signal.
        """
        signal.signal(signalnum, handler)

    def _on_signal_callback(self, signalnum: int, frame: FrameType | None) -> None:
        signal.signal(signal.SIGTERM, signal.SIG_IGN)
        signal.signal(signal.SIGINT, signal.SIG_IGN)
        logger.info("Received %s, shutting down gracefully", signal_names[signalnum])
        self.interrupt()

    def interrupt(self) -> None:
        """
        Cancel the consumer, which causes the threads to terminate and the connection to close.
        """
        self.channel.stop_consuming(self.consumer_tag)

    def close(self) -> None:
        """
        Close the connection: for example, after sending messages from a simple publisher.
        """
        self.connection.close()


class Async(Base[AsyncioConnection]):
    """
    Uses Pika's :class:`AsyncioConnection adapter<pika.adapters.asyncio_connection.AsyncioConnection>`. Reconnects to
    RabbitMQ if the connection is closed unexpectedly or can't be established.

    Calling :meth:`~yapw.clients.Async.start` connects to RabbitMQ, add signal handlers, and starts the IO loop.

    The signal handlers cancel the consumer, if consuming and if the channel is open. Otherwise, they wait for threads
    to terminate and close the connection. This also occurs if the broker cancels the consumer or if the channel closes
    for any other reason.

    Once the IO loop starts, the client creates a channel, sets the prefetch count, and declares the
    exchange. Once the exchange is declared, the :meth:`~yapw.clients.Async.exchange_declareok_callback` calls
    :meth:`~yapw.clients.Async.exchange_ready`. You must define this method in a subclass, to do any work you need.

    If the connection becomes `blocked <https://www.rabbitmq.com/connection-blocked.html>`__ or unblocked, the
    client's ``blocked`` attribute is set to ``True`` or ``False``, respectively. Your code can use this attribute to,
    for example, pause, buffer or reschedule deliveries.

    If you subclass this client and add and mutate any attributes, override :meth:`~yapw.clients.Async.reset`.

    .. seealso::

       -  If your code runs subprocesses, be familiar with asyncio's :py:ref:`asyncio-subprocess`.
       -  If your code configures logging, see :py:ref:`blocking-handlers`.
    """

    # RabbitMQ takes about 10 seconds to restart.
    RECONNECT_DELAY = 15

    def __init__(self, **kwargs: Any):
        """
        Initialize the client's state.
        """
        super().__init__(**kwargs)

        #: The thread pool executor.
        self.executor = ThreadPoolExecutor(thread_name_prefix=f"yapw-{self.thread_name_infix}")
        #: Whether the connection is `blocked <https://www.rabbitmq.com/connection-blocked.html>`__.
        self.blocked = False
        #: Whether the client is being stopped deliberately.
        self.stopping = False

    @property
    def thread_name_infix(self) -> str:
        """
        Return the exchange name to use as part of the thread name.
        """
        return self.exchange

    def start(self) -> None:
        """
        :meth:`Connect<yapw.clients.Async.connect>` to RabbitMQ, add signal handlers, and start the IO loop.
        """
        self.connect()
        self.add_signal_handlers(self._on_signal_callback)
        self.connection.ioloop.call_soon_threadsafe(print)
        self.connection.ioloop.run_forever()

    def connect(self) -> None:
        """
        Connect to RabbitMQ, create a channel, set the prefetch count, and declare an exchange, unless using the
        default exchange.
        """
        self.connection = AsyncioConnection(
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
            self.reset()
            self.connect()

    def reset(self) -> None:
        """
        Reset the client's state, before reconnecting.

        Override this method in subclasses, if your subclass adds and mutates any attributes.
        """
        self.executor = ThreadPoolExecutor(thread_name_prefix=f"yapw-{self.thread_name_infix}")
        self.blocked = False
        self.consumer_tag = ""

    def connection_open_error_callback(self, connection: pika.connection.Connection, error: Exception | str) -> None:
        """Retry, once the connection couldn't be established."""
        if isinstance(error, (ProbableAccessDeniedError, ProbableAuthenticationError)):
            logger.error("Stopping: %r", error)
            self.connection.ioloop.stop()
        else:
            logger.error("Connection failed, retrying in %ds: %r", self.RECONNECT_DELAY, error)
            self.connection.ioloop.call_later(self.RECONNECT_DELAY, self.reconnect)

    def connection_close_callback(self, connection: pika.connection.Connection, reason: Exception) -> None:
        """Reconnect, if the connection was closed unexpectedly. Otherwise, stop the IO loop."""
        if self.stopping:
            # A message has been logged, prior to calling interrupt().
            self.connection.ioloop.stop()
        else:
            # For example: ConnectionClosedByBroker: (320) "CONNECTION_FORCED - broker forced connection closure with
            # reason 'shutdown'"
            logger.warning("Connection closed, reconnecting in %ds: %r", self.RECONNECT_DELAY, reason)
            self.connection.ioloop.call_later(self.RECONNECT_DELAY, self.reconnect)

    def add_signal_handler(self, signalnum: int, handler: Callable[..., object]) -> None:
        """
        Add a handler for a signal.
        """
        self.connection.ioloop.add_signal_handler(signalnum, functools.partial(handler, signalnum=signalnum))

    def _on_signal_callback(self, signalnum: int) -> None:
        if not self.stopping:  # remove_signal_handler() is too slow
            self.connection.ioloop.remove_signal_handler(signal.SIGTERM)
            self.connection.ioloop.remove_signal_handler(signal.SIGINT)
            logger.info("Received %s, shutting down gracefully", signal_names[signalnum])
            self.interrupt()

    def interrupt(self) -> None:
        """
        `Cancel <https://www.rabbitmq.com/consumers.html#unsubscribing>`__ the consumer if consuming and if the channel
        is open. Otherwise, wait for threads to terminate and close the connection.
        """
        # Change the client's state to stopping, to prevent infinite reconnection.
        self.stopping = True

        if self.consumer_tag and not self.channel.is_closed and not self.channel.is_closing:
            self.channel.basic_cancel(self.consumer_tag, self.channel_cancelok_callback)
        elif not self.connection.is_closed and not self.connection.is_closing:
            # The channel is already closed. Free any resources, without waiting for threads.
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
        redeclaring exchanges with inconsistent parameters.

        A warning is logged, in case it was the latter.
        """
        logger.warning("Channel %i was closed: %r", channel, reason)
        # pika's connection.close() closes all channels. It can update the connection state before this callback runs.
        if not self.connection.is_closed and not self.connection.is_closing:
            # The channel is already closed. Free any resources, without waiting for threads.
            self.executor.shutdown(wait=False, cancel_futures=True)
            self.connection.close()

    def channel_qosok_callback(self, method: pika.frame.Method[pika.spec.Basic.QosOk]) -> None:
        """Declare the exchange, once the prefetch count is set."""
        if self.exchange:
            logger.debug(
                "Declaring %s %s exchange %s",
                "durable" if self.durable else "transient",
                self.exchange_type,
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
        """Override this method in subclasses, which is called once the exchange is declared."""
        raise NotImplementedError  # causes StreamLostError if unimplemented


class AsyncConsumer(Async):
    """
    An asynchronous consumer, extending :class:`~yapw.clients.Async`.

    After calling :meth:`~yapw.clients.Async.start`, this client declares the ``queue``, binds it to the exchange with
    the ``routing_keys``, and starts consuming messages from that queue, using the ``on_message_callback``.

    The ``on_message_callback`` and ``queue`` keyword arguments are required. If no ``routing_keys`` are provided, the
    ``queue`` is bound to the exchange using its name as the routing key.

    The :meth:`pika.channel.Channel.basic_consume` call sets its callback to an empty method,
    :meth:`~yapw.clients.AsyncConsumer.channel_consumeok_callback`. Define this method in a subclass, if needed.
    """

    def __init__(
        self,
        *,
        on_message_callback: ConsumerCallback,
        queue: str,
        routing_keys: list[str] | None = None,
        decorator: Decorator = halt,
        arguments: dict[str, str] | None = None,
        **kwargs: Any,
    ) -> None:
        """
        .. seealso::

           :meth:`yapw.clients.AsyncConsumer.consume`

        :param on_message_callback: the consumer callback
        :param queue: the queue's name
        :param routing_keys: the queue's routing keys
        :param decorator: the decorator of the consumer callback
        :param arguments: the ``arguments`` parameter to the ``queue_declare`` method
        """
        #: The queue's name.
        self.queue = queue
        #: The queue's routing keys.
        self.routing_keys = routing_keys or [queue]
        #: The ``arguments`` parameter to the ``queue_declare`` method.
        self.arguments = arguments
        #: The consumer callback.
        self.on_message_callback = on_message_callback
        #: The decorator of the consumer callback.
        self.decorator = decorator

        # self.queue must be set for the thread_name_infix() call in the super() method.
        super().__init__(**kwargs)

    @property
    def thread_name_infix(self) -> str:
        """
        Return the queue name to use as part of the thread name.
        """
        return self.queue

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
            self.consume(self.on_message_callback, self.decorator, queue_name)

    def _bind_queue(self, queue_name: str, index: int) -> None:
        routing_key = self.format_routing_key(self.routing_keys[index])
        cb = functools.partial(self.queue_bindok_callback, queue_name=queue_name, index=index + 1)
        self.channel.queue_bind(queue=queue_name, exchange=self.exchange, routing_key=routing_key, callback=cb)

    def consume(self, on_message_callback: ConsumerCallback, decorator: Decorator, queue_name: str) -> None:
        """
        Start consuming messages from the queue.

        Run the consumer callback in separate threads, to not block the IO loop. (This assumes the consumer callback is
        :py:ref:`CPU-bound<asyncio-handle-blocking>`.) Add signal handlers to wait for threads to terminate.

        The consumer callback is a function that accepts ``(state, channel, method, properties, body)`` arguments. The
        ``state`` argument contains thread-safe attributes. The rest of the arguments are the same as
        :meth:`pika.channel.Channel.basic_consume`'s ``on_message_callback``.
        """
        self.channel.add_on_cancel_callback(self.channel_cancel_callback)

        submit = functools.partial(self.connection.ioloop.run_in_executor, self.executor)
        cb = functools.partial(_on_message, args=(submit, decorator, self.decode, on_message_callback, self.state))

        self.consumer_tag = self.channel.basic_consume(queue_name, cb, callback=self.channel_consumeok_callback)
        logger.debug("Consuming messages on channel %s from queue %s", self.channel.channel_number, queue_name)

    def channel_cancel_callback(self, method: Any) -> Any:  # https://github.com/qubidt/types-pika/pull/15
        """
        Close the channel.

        RabbitMQ uses `basic.cancel <https://www.rabbitmq.com/consumer-cancel.html>`__ if a channel is consuming a
        queue and the queue is deleted.
        """
        logger.error("Consumer was cancelled by broker, stopping: %r", method)
        # Keep channel open until threads terminate. Ensure the channel closes after any thread-safe callbacks.
        self.executor.shutdown(cancel_futures=True)
        self.connection.ioloop.call_later(0, self.channel.close)

    def channel_consumeok_callback(self, method: pika.frame.Method[pika.spec.Basic.ConsumeOk]) -> None:
        """
        Override this method in subclasses to perform any other work, once the consumer is started.
        """
        pass
