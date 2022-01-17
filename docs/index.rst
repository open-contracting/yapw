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

Create a Client class, by layering in :doc:`mixins<api/clients>`:

.. code-block:: python

   from yapw import clients


   class Client(clients.Threaded, clients.Durable, clients.Blocking, clients.Base):
       pass

Each mixing contributes features, such that a client will:

-  :class:`~yapw.clients.Blocking`: Use `pika.BlockingConnection <https://pika.readthedocs.io/en/stable/modules/adapters/blocking.html>`__, while avoiding deadlocks by setting ``blocked_connection_timeout`` to a sensible default.
-  :class:`~yapw.clients.Durable`: Declare a durable exchange, use persistent messages on :meth:`~yapw.clients.Durable.publish`, and create a durable queue on :meth:`~yapw.clients.Threaded.consume`.
-  :class:`~yapw.clients.Threaded`: Run the consumer callback in separate threads when consuming messages. Install handlers for the SIGTERM, SIGINT and user-defined signals to stop consuming messages, wait for threads to terminate, and close the connection.

Publish messages outside a consumer callback
--------------------------------------------

.. code-block:: python

   publisher = Client(url="amqp://user:pass@127.0.0.1", exchange="myexchange")
   publisher.publish({"message": "value"}, routing_key="messages")

The routing key is namespaced by the exchange name, to make it "myexchange_messages".

Consume messages
----------------

.. code-block:: python

   from yapw.decorators import discard
   from yapw.methods.blocking import ack, nack, publish


   def callback(state, channel, method, properties, body):
       try:
           key = json.loads(body)["key"]
           # do work
           publish(state, channel, {"message": "value"}, "myroutingkey")
       except KeyError:
           nack(state, channel, method.delivery_tag)
       finally:
           ack(state, channel, method.delivery_tag)


   consumer = Client(url="amqp://user:pass@127.0.0.1", exchange="myexchange", prefetch_count=5)
   consumer.consume(callback, queue="messages", decorator=discard)

yapw implements a pattern whereby the consumer declares and binds a queue. By default, the queue's name and binding key are the same, and are namespaced by the exchange name. To set the binding keys:

.. code-block:: python

   consumer.consume(callback, queue="messages", routing_keys=["a", "b"], decorator=discard)

.. seealso::

   :meth:`yapw.clients.Threaded.consume` for details on the consumer callback function signature.

Channel methods
~~~~~~~~~~~~~~~

The :func:`~yapw.methods.blocking.ack`, :func:`~yapw.methods.blocking.nack` and  :func:`~yapw.methods.blocking.publish` functions are safe to call from the consumer callback. They log an error if the connection or channel isn't open.

.. note::

   Thread-safe helper functions (using `add_callback_threadsafe() <https://pika.readthedocs.io/en/stable/modules/adapters/blocking.html#pika.adapters.blocking_connection.BlockingConnection.add_callback_threadsafe>`__) have not yet been defined for all relevant `channel methods <https://pika.readthedocs.io/en/stable/modules/adapters/blocking.html#pika.adapters.blocking_connection.BlockingChannel>`__.

Encoding and decoding
~~~~~~~~~~~~~~~~~~~~~

By default, when publishing messages, the :class:`~yapw.clients.Durable` and :class:`~yapw.clients.Transient` mixins use a content type of "application/json" and encode the message body with the :func:`~yapw.util.default_encode` function, which serializes to JSON-formatted bytes when the content type is "application/json".

Similarly, when consuming messages, the :class:`yapw.clients.Threaded` mixin uses the :func:`~yapw.decorators.default_decode` function, which deserializes from JSON-formatted bytes when the consumed message's content type is "application/json".

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


   client = Client(encode=encode, decode=decode)
   publisher.publish({"message": "value"}, routing_key="messages")

Error handling
~~~~~~~~~~~~~~

The ``decorator`` keyword argument to the :meth:`~yapw.clients.Threaded.consume` method is a function that wraps the consumer callback (the first argument to the ``consume`` method). This function can be used to:

-  Offer conveniences to the consumer callback, like decoding the message body
-  Handle unexpected errors from the consumer callback

When using `consumer prefetch <https://www.rabbitmq.com/consumer-prefetch.html>`__, if a message is not ack'd or nack'd, then `RabbitMQ stops delivering messages <https://www.rabbitmq.com/confirms.html#channel-qos-prefetch>`__. As such, it's important to handle unexpected errors by either acknowledging the message or halting the process. Otherwise, the process will stall.

The default decorator is the :func:`yapw.decorators.halt` function, which sends the SIGUSR1 signal to the main thread, without acknowledging the message. The :class:`~yapw.clients.Threaded` mixin handles this signal by shutting down gracefully. See the :doc:`available decorators<api/decorators>` and the rationale for the default setting.

All decorators also decode the message body, which can be configured as above. If an exception occurs while decoding, the decorator sends the SIGUSR2 signal to the main thread, without acknowledging the message. The :class:`~yapw.clients.Threaded` mixin handles this signal by shutting down gracefully.

Signal handling
~~~~~~~~~~~~~~~

The :class:`~yapw.clients.Threaded` mixin shuts down gracefully if it receives the ``SIGTERM`` (system exit), ``SIGINT`` (keyboard interrupt) or user-defined signals described above. It stops consuming messages, waits for threads to terminate, and closes the RabbitMQ connection.

Copyright (c) 2021 Open Contracting Partnership, released under the BSD license
