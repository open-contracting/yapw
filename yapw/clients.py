"""
Mixins that can be composed in layers. For example:

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

from yapw.decorators import halt
from yapw.ossignal import install_signal_handlers, signal_names
from yapw.util import basic_publish_debug_args, basic_publish_kwargs

logger = logging.getLogger(__name__)

# Pika is verbose.
logging.getLogger("pika").setLevel(logging.WARNING)


def _on_message(channel, method, properties, body, args):
    (state, threads, callback, decorator) = args
    thread = threading.Thread(target=decorator, args=(callback, state, channel, method, properties, body))
    thread.start()
    threads.append(thread)


class Base:
    """
    Provides :meth:`~Base.format_routing_key`, which is used by all methods in other mixins that accept routing keys,
    in order to namespace the routing keys.

    Attributes that can - and are expected to be - used safely in consumer callbacks should be listed in a ``__safe__``
    class attribute.
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
        Returns the formatted routing key.

        :param str routing_key: the routing key
        """
        return self.routing_key_template.format(routing_key=routing_key, **self.__dict__)

    @property
    @functools.lru_cache(maxsize=None)
    def __getsafe__(self):
        """
        Returns the attributes that can be used safely in consumer callbacks across all base classes and this class.
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
        Connects to RabbitMQ and creates a channel.

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
        Closes the connection.
        """
        self.connection.close()


class Publisher:
    """
    An abstract parent class. Use :class:`~yapw.clients.Durable` or :class:`~yapw.clients.Transient` instead.
    """

    durable = None
    delivery_mode = None

    __safe__ = ["exchange", "delivery_mode"]

    def __init__(
        self, *, exchange="", exchange_type="direct", routing_key_template="{exchange}_{routing_key}", **kwargs
    ):
        """
        Declares an exchange, unless using the default exchange.

        :param str exchange: the exchange name
        """
        super().__init__(routing_key_template=routing_key_template, **kwargs)

        #: The exchange name.
        self.exchange = exchange

        if self.exchange:
            self.channel.exchange_declare(exchange=self.exchange, exchange_type=exchange_type, durable=self.durable)

    def declare_queue(self, routing_key):
        """
        Declares a queue named after the routing key, and binds it to the exchange with the routing key.

        :param str routing_key: the routing key
        """
        formatted = self.format_routing_key(routing_key)

        self.channel.queue_declare(queue=formatted, durable=self.durable)
        self.channel.queue_bind(exchange=self.exchange, queue=formatted, routing_key=formatted)

    def publish(self, message, routing_key):
        """
        Publishes from the main thread, with the provided message and routing key, and with the configured exchange.

        :param message: a JSON-serializable message
        :param str routing_key: the routing key
        """
        keywords = basic_publish_kwargs(self, message, routing_key)

        self.channel.basic_publish(**keywords)
        logger.debug(*basic_publish_debug_args(message, keywords))


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

    def __init__(self, **kwargs):
        """
        Installs handlers for the SIGTERM and SIGINT signals to stop consuming messages, wait for threads to terminate,
        and close the connection.
        """
        super().__init__(**kwargs)

        install_signal_handlers(self._on_shutdown)

    def consume(self, callback, routing_key, decorator=halt):
        """
        Declares a queue named after and bound by the routing key, and starts consuming messages from that queue,
        dispatching messages to the decorated callback.

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
        on_message_callback = functools.partial(_on_message, args=(state, threads, callback, decorator))
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
