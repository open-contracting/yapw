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

import functools
import logging
import signal
import threading
from collections import namedtuple

import pika

from yapw.decorators import default_decode, halt
from yapw.ossignal import install_signal_handlers, signal_names
from yapw.util import basic_publish_debug_args, basic_publish_kwargs, default_encode

logger = logging.getLogger(__name__)

# Pika is verbose.
logging.getLogger("pika").setLevel(logging.WARNING)


def _on_message(channel, method, properties, body, args):
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

    def __init__(self, *, routing_key_template="{routing_key}", **kwargs):
        """
        :param str routing_key_template:
            a `format string <https://docs.python.org/3/library/string.html#format-string-syntax>`__ that must contain
            the ``{routing_key}`` replacement field and that may contain other fields matching writable attributes
        """
        #: The format string for the routing key.
        self.routing_key_template = routing_key_template

    def format_routing_key(self, routing_key):
        """
        Format the routing key.

        :param str routing_key: the routing key
        :returns: the formatted routing key
        :rtype: str
        """
        return self.routing_key_template.format(routing_key=routing_key, **self.__dict__)

    @property
    @functools.lru_cache(maxsize=None)
    def __getsafe__(self):
        """
        Return the attributes that can be used safely in consumer callbacks across all base classes and this class.

        :returns: the attributes that can be used safely in consumer callbacks
        :rtype: set
        """
        return {attr for base in type(self).__bases__ for attr in getattr(base, "__safe__", [])} | set(
            type(self).__safe__
        )


class Blocking:
    """
    Uses a `blocking connection <https://pika.readthedocs.io/en/stable/modules/adapters/blocking.html>`__ while
    avoiding deadlocks due to `blocked connections <https://www.rabbitmq.com/connection-blocked.html>`__.
    """

    # The connection isn't "safe to use" but it can be "used safely" like in:
    # https://github.com/pika/pika/blob/master/examples/basic_consumer_threaded.py
    __safe__ = ["connection"]

    def __init__(self, *, url="amqp://127.0.0.1", blocked_connection_timeout=1800, prefetch_count=1, **kwargs):
        """
        Connect to RabbitMQ and create a channel.

        :param str url: the connection string (don't set a blocked_connection_timeout query string parameter)
        :param int blocked_connection_timeout: the timeout, in seconds, that the connection may remain blocked
        :param int prefetch_count: the maximum number of unacknowledged deliveries that are permitted on the channel
        """
        super().__init__(**kwargs)

        parameters = pika.URLParameters(url)
        parameters.blocked_connection_timeout = blocked_connection_timeout

        #: The connection.
        self.connection = pika.BlockingConnection(parameters)

        #: The channel.
        self.channel = self.connection.channel()
        self.channel.basic_qos(prefetch_count=prefetch_count)

    def close(self):
        """
        Close the connection.
        """
        self.connection.close()


class Publisher:
    """
    An abstract parent class. Use :class:`~yapw.clients.Durable` or :class:`~yapw.clients.Transient` instead.
    """

    durable = None
    delivery_mode = None

    __safe__ = ["exchange", "encode", "content_type", "delivery_mode"]

    def __init__(
        self,
        *,
        exchange="",
        exchange_type="direct",
        encode=default_encode,
        content_type="application/json",
        routing_key_template="{exchange}_{routing_key}",
        **kwargs
    ):
        """
        Declare an exchange, unless using the default exchange.

        When publishing a message, by default, its body is encoded using :func:`yapw.util.default_encode`, and its
        content type is set to "application/json". Use the ``encode`` and ``content_type`` keyword arguments to change
        this. The ``encode`` must be a function that accepts ``(message, content_type)`` arguments and returns bytes.

        :param str exchange: the exchange name
        :param encode: the message body's encoder
        :param str content_type: the message's content type
        """
        super().__init__(routing_key_template=routing_key_template, **kwargs)

        #: The exchange name.
        self.exchange = exchange
        #: The message body's encoder.
        self.encode = encode
        #: The message's content type.
        self.content_type = content_type

        if self.exchange:
            self.channel.exchange_declare(exchange=self.exchange, exchange_type=exchange_type, durable=self.durable)

    def declare_queue(self, routing_key):
        """
        Declare a queue named after the routing key, and bind it to the exchange with the routing key.

        :param str routing_key: the routing key
        """
        formatted = self.format_routing_key(routing_key)

        self.channel.queue_declare(queue=formatted, durable=self.durable)
        self.channel.queue_bind(exchange=self.exchange, queue=formatted, routing_key=formatted)

    def publish(self, message, routing_key):
        """
        Publish from the main thread, with the provided message and routing key, and with the configured exchange.

        :param message: a decoded message
        :param str routing_key: the routing key
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

    def __init__(self, decode=default_decode, **kwargs):
        """
        Install signal handlers to stop consuming messages, wait for threads to terminate, and close the connection.

        When consuming a message, by default, its body is decoded using :func:`yapw.decorators.default_decode`. Use the
        ``decode`` keyword argument to change this. The ``decode`` must be a function that accepts ``(state, channel,
        method, properties, body)`` arguments (like the consumer callback) and returns a decoded message.

        :param decode: the message body's decoder
        """
        super().__init__(**kwargs)

        #: The message body's decoder.
        self.decode = decode

        install_signal_handlers(self._on_shutdown)

    def consume(self, callback, routing_key, decorator=halt):
        """
        Declare a queue named after and bound by the routing key, and start consuming messages from that queue.

        The consumer callback must be a function that accepts ``(state, channel, method, properties, body)`` arguments,
        all but the first of which are the same as Pika's ``basic_consume``. The ``state`` argument is needed to pass
        attributes to :mod:`yapw.methods` functions.

        :param callback: the consumer callback
        :param str routing_key: the routing key
        :param decorator: the decorator of the consumer callback
        """
        formatted = self.format_routing_key(routing_key)

        self.declare_queue(routing_key)

        # Don't pass `self` to the callback, to prevent use of unsafe attributes and mutation of safe attributes.
        State = namedtuple("State", self.__getsafe__)
        state = State(**{attr: getattr(self, attr) for attr in self.__getsafe__})

        threads = []
        on_message_callback = functools.partial(_on_message, args=(threads, decorator, self.decode, callback, state))
        self.channel.basic_consume(formatted, on_message_callback)

        logger.debug("Consuming messages on channel %s from queue %s", self.channel.channel_number, formatted)
        try:
            self.channel.start_consuming()
        finally:
            for thread in threads:
                thread.join()
            self.connection.close()

    def _on_shutdown(self, signum, frame):
        install_signal_handlers(signal.SIG_IGN)
        logger.info("Received %s, shutting down gracefully", signal_names[signum])
        self.channel.stop_consuming()
