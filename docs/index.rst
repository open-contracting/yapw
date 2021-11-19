Yet Another Pika Wrapper |release|
==================================

.. include:: ../README.rst

.. toctree::
   :caption: Contents

   api/index
   contributing/index
   changelog

Basic usage
-----------

Create a Client class, by layering in :doc:`mixins<api/clients>`. Each mixing contributes features, such that the client will:

-  :class:`~pika.clients.Blocking`: Use `pika.BlockingConnection <https://pika.readthedocs.io/en/stable/modules/adapters/blocking.html>`__, while avoiding deadlocks by setting ``blocked_connection_timeout`` to a sensible default.
-  :class:`~pika.clients.Durable`: Declare a durable exchange, use persistent messages on :meth:`~pika.clients.Durable.publish`, and create a durable queue on :meth:`~pika.clients.Threaded.consume`.
-  :class:`~pika.clients.Threaded`: Run the consumer callback in separate threads when consuming messages, and handle the SIGTERM and SIGINT signals by stopping consuming messages, waiting for threads to terminate, and closing the connection.

.. code-block:: python

   from yapw import clients


   class Client(clients.Threaded, clients.Durable, clients.Blocking, clients.Base):
       pass

Create a publisher:

.. code-block:: python

   publisher = Client(url="amqp://user:pass@127.0.0.1", exchange="myexchange")
   publisher.publish({"message": "value"}, "messages")

The routing key is namespaced by the exchange name, to make it "myexchange_messages".

Create a consumer:

.. code-block:: python

   from yapw.decorators import rescue
   from yapw.methods import ack, nack


   def callback(connection, channel, method, properties, body):
       try:
           json.loads(body)["key"]
       except KeyError:
           nack(connection, channel, method.delivery_tag)
       finally:
           ack(connection, channel, method.delivery_tag)


   consumer = Client(url="amqp://user:pass@127.0.0.1", exchange="myexchange", prefetch_count=5)
   consumer.consume(callback, "messages", decorator=rescue)

The ``decorator`` keyword argument controls how the message is acknowledged if an unexpected error occurs. See the :doc:`available decorators<api/decorators>`.

yapw implements a pattern whereby consumers declare and bind a queue. The queue's name and binding key are the same, and are namespaced by the exchange name, to make them "myexchange_messages".

The :func:`~pika.methods.ack` and :func:`~pika.methods.nack` methods are safe to call from the consumer callback, and log an error if the connection or channel isn't open.

Copyright (c) 2021 Open Contracting Partnership, released under the BSD license
