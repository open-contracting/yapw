Yet Another Pika Wrapper |release|
==================================

.. include:: ../README.rst

.. toctree::
   :caption: Contents
   :maxdepth: 1

   api/index
   contributing/index
   changelog

Configure a RabbitMQ client
---------------------------

Import a client class, for example:

.. tab-set::

   .. tab-item:: Blocking
      :sync: blocking

      .. code-block:: python

         from yapw.clients import Blocking

   .. tab-item:: Asynchronous
      :sync: async

      .. code-block:: python

         from yapw.clients import AsyncConsumer
         # Or, if the client will exclusively publish messages:
         from yapw.clients import Async

Publish messages outside a consumer callback
--------------------------------------------

.. tab-set::

   .. tab-item:: Blocking
      :sync: blocking

      .. code-block:: python

         from yapw.clients import Blocking


         publisher = Blocking(url="amqp://user:pass@127.0.0.1", exchange="myexchange")
         publisher.publish({"message": "value"}, routing_key="messages")
         publisher.close()

   .. tab-item:: Asynchronous
      :sync: async

      .. code-block:: python

         from yapw.clients import Async


         class Publisher(Async):
             def exchange_ready(self):
                 self.publish({"message": "value"}, routing_key="messages")
                 self.interrupt()  # only if you want to stop the client


         publisher = Publisher(url="amqp://user:pass@127.0.0.1", exchange="myexchange")
         publisher.start()

The routing key is namespaced by the exchange name, to make it "myexchange_messages".

Consume messages
----------------

.. tab-set::

   .. tab-item:: Blocking
      :sync: blocking

      .. code-block:: python

         from yapw.clients import Blocking
         from yapw.decorators import discard
         from yapw.methods import ack, nack, publish


         def callback(state, channel, method, properties, body):
             try:
                 key = body["key"]
                 # do work
                 publish(state, channel, {"message": "value"}, "myroutingkey")
             except KeyError:
                 nack(state, channel, method.delivery_tag)
             else:
                 ack(state, channel, method.delivery_tag)


         consumer = Blocking(
             url="amqp://user:pass@127.0.0.1",
             exchange="myexchange",
             prefetch_count=5,
         )
         consumer.consume(callback, queue="messages", decorator=discard)

   .. tab-item:: Asynchronous
      :sync: async

      .. code-block:: python

         from yapw.clients import AsyncConsumer
         from yapw.decorators import discard
         from yapw.methods import ack, nack, publish


         def callback(state, channel, method, properties, body):
             try:
                 key = body["key"]
                 # do work
                 publish(state, channel, {"message": "value"}, "myroutingkey")
             except KeyError:
                 nack(state, channel, method.delivery_tag)
             else:
                 ack(state, channel, method.delivery_tag)


         consumer = AsyncConsumer(
             url="amqp://user:pass@127.0.0.1",
             exchange="myexchange",
             prefetch_count=5,
             on_message_callback=callback,
             queue="messages",
             decorator=discard,
         )
         consumer.start()

yapw implements a pattern whereby the consumer declares and binds a queue. By default, the queue's name and binding key are the same, and are namespaced by the exchange name.

To manually set the binding keys:

.. tab-set::

   .. tab-item:: Blocking
      :sync: blocking

      .. code-block:: python
         :emphasize-lines: 4

         consumer.consume(
             callback,
             queue="messages",
             routing_keys=["a", "b"],
             decorator=discard,
         )

   .. tab-item:: Asynchronous
      :sync: async

      .. code-block:: python
         :emphasize-lines: 5

         consumer = AsyncConsumer(
             # ...
             on_message_callback=callback,
             queue="messages",
             routing_keys=["a", "b"],
             decorator=discard
         )

yapw uses a thread pool to run the consumer callback in separate threads.

.. seealso::

   :mod:`yapw.clients` for details on the consumer callback function signature.

Channel methods
~~~~~~~~~~~~~~~

The :func:`~yapw.methods.ack`, :func:`~yapw.methods.nack` and  :func:`~yapw.methods.publish` functions are safe to call from a consumer callback running in another thread. They log an error if the connection or channel isn't open.

.. note::

   Thread-safe helper functions have not yet been defined for all relevant :class:`channel methods<pika.channel.Channel>`.

Encoding and decoding
~~~~~~~~~~~~~~~~~~~~~

By default, when publishing messages, the content type of "application/json" is used, the message body is encoded using the :func:`~yapw.util.default_encode` function, which serializes to JSON-formatted bytes when the content type is "application/json".

Similarly, when consuming messages, the :func:`~yapw.util.default_decode` function is used, which deserializes from JSON-formatted bytes when the consumed message's content type is "application/json". (That is how the sample code above can read ``body["key"]`` without decoding.)

You can change this behavior. For example, change the bodies of the ``encode`` and ``decode`` functions below:

.. code-block:: python

   import json


   # Return bytes.
   class encode(message, content_type):
       if content_type == "application/json":
           return json.dumps(message).encode()
       return message


   # Accept body as bytes.
   class decode(body, content_type):
       if content_type == "application/json":
           return json.loads(body)
       return body


   client = Blocking(encode=encode, decode=decode)

Error handling
~~~~~~~~~~~~~~

The ``decorator`` keyword argument to the :meth:`~yapw.clients.Blocking.consume` method is a function that wraps the consumer callback (the first argument to the ``consume`` method). This function can be used to:

-  Offer conveniences to the consumer callback, like decoding the message body
-  Handle unexpected errors from the consumer callback

.. note::

   For the :class:`~yapw.clients.AsyncConsumer` client, the ``decorator`` and ``callback`` arguments are passed to its :meth:`~yapw.clients.AsyncConsumer.__init__` method.

When using `consumer prefetch <https://www.rabbitmq.com/consumer-prefetch.html>`__, if a message is not ack'd or nack'd, then `RabbitMQ stops delivering messages <https://www.rabbitmq.com/confirms.html#channel-qos-prefetch>`__. As such, it's important to handle unexpected errors by either acknowledging the message or halting the process. Otherwise, the process will stall.

The default decorator is the :func:`yapw.decorators.halt` function, which shuts down the client in the main thread, without acknowledging the message. See the :doc:`available decorators<api/decorators>` and the rationale for the default setting.

All decorators also decode the message body, which can be configured as above. If an exception occurs while decoding, the decorator shuts down the client in the main thread, without acknowledging the message.

Signal handling
~~~~~~~~~~~~~~~

Every client shuts down gracefully if it receives the ``SIGTERM`` (system exit) or ``SIGINT`` (keyboard interrupt) signals. It stops consuming messages, waits for threads to terminate, and closes the RabbitMQ connection.

Signal handlers are added only if a client is instantiated in the main thread, because `only the main thread is allowed to set a new signal handler <https://docs.python.org/3/library/signal.html#signals-and-threads>`__. An example of a non-main thread is a web request.

Copyright (c) 2021 Open Contracting Partnership, released under the BSD license
