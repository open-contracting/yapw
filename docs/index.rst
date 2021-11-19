Yet Another Pika Wrapper |release|
==================================

.. include:: ../README.rst

.. toctree::
   :caption: Contents
   :maxdepth: 1

   api/index
   contributing/index
   changelog

Basic usage
-----------

Create a Client class, by layering in :doc:`mixins<api/clients>`:

.. code-block:: python

   from yapw import clients


   class Client(clients.Threaded, clients.Durable, clients.Blocking, clients.Base):
       pass

Each mixing contributes features, such that a client will:

-  :class:`~pika.clients.Blocking`: Use `pika.BlockingConnection <https://pika.readthedocs.io/en/stable/modules/adapters/blocking.html>`__, while avoiding deadlocks by setting ``blocked_connection_timeout`` to a sensible default.
-  :class:`~pika.clients.Durable`: Declare a durable exchange, use persistent messages on :meth:`~pika.clients.Durable.publish`, and create a durable queue on :meth:`~pika.clients.Threaded.consume`.
-  :class:`~pika.clients.Threaded`: Run the consumer callback in separate threads when consuming messages. Install handlers for the SIGTERM and SIGINT signals to stop consuming messages, wait for threads to terminate, and close the connection.

Create a publisher:

.. code-block:: python

   publisher = Client(url="amqp://user:pass@127.0.0.1", exchange="myexchange")
   publisher.publish({"message": "value"}, routing_key="messages")

The routing key is namespaced by the exchange name, to make it "myexchange_messages".

Create a consumer:

.. code-block:: python

   from yapw.decorators import discard
   from yapw.methods import ack, nack, publish


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

The ``decorator`` keyword argument controls how the message is acknowledged if an unexpected error occurs. See the :doc:`available decorators<api/decorators>`.

yapw implements a pattern whereby the consumer declares and binds a queue. The queue's name and binding key are the same, and are namespaced by the exchange name.

The :func:`~pika.methods.ack`, :func:`~pika.methods.nack` and  :func:`~pika.methods.publish` methods are safe to call from the consumer callback. They log an error if the connection or channel isn't open.

Copyright (c) 2021 Open Contracting Partnership, released under the BSD license
