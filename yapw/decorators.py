"""
Decorators to be used with consumer callbacks.

A message must be ack'd or nack'd if using `consumer prefetch <https://www.rabbitmq.com/consumer-prefetch.html>`__,
because otherwise `RabbitMQ stops delivering messages <https://www.rabbitmq.com/confirms.html#channel-qos-prefetch>`__.
The decorators help to ensure that, in case of error, either the message is nack'd or the process is halted.

:func:`~yapw.decorators.halt` is the default decorator. It stops the consumer and halts the process, so that an
administrator can decide when it is appropriate to restart it.

The other decorators require more care. For example, if a callback inserts messages into a database, and the database
is down, but this exception isn't handled by the callback, then the :func:`~yapw.decorators.discard` or
:func:`~yapw.decorators.requeue` decorators would end up nack'ing all messages in the queue.

Decorators look like this (see the :func:`~yapw.decorators.decorate` function for context):

.. code-block:: python

   from yapw.decorators import decorate


   def myfunction(decode, callback, state, channel, method, properties, body):
       def errback(exception):
           # do something, like halting the process or nack'ing the message

       decorate(decode, callback, state, channel, method, properties, body, errback)
"""
from __future__ import annotations

import logging
from collections.abc import Callable
from typing import Any

import pika

from yapw.methods import add_callback_threadsafe, nack
from yapw.types import ConsumerCallback, Decode, State

logger = logging.getLogger(__name__)


def decorate(
    decode: Decode,
    callback: ConsumerCallback,
    state: State[Any],
    channel: pika.channel.Channel,
    method: pika.spec.Basic.Deliver,
    properties: pika.BasicProperties,
    body: bytes,
    errback: Callable[[Exception], None],
    finalback: Callable[[], None] | None = None,
) -> None:
    """
    Use this function to define your own decorators.

    Decode the message ``body`` using the ``decode`` function, and call the consumer ``callback``.

    If the ``callback`` function raises an exception, call the ``errback`` function. In any case, call the
    ``finalback`` function after calling the ``callback`` function.

    If the ``decode`` function raises an exception, shut down the client in the main thread.

    .. seealso::

       :mod:`yapw.clients` for details on the consumer callback function signature.
    """
    logger.debug(
        "Received message %s with routing key %s and delivery tag %s", body, method.routing_key, method.delivery_tag
    )
    try:
        message = decode(body, properties.content_type)
        try:
            callback(state, channel, method, properties, message)
        except Exception as exception:
            errback(exception)
        finally:
            if finalback:
                finalback()
    except Exception:
        logger.exception("%r can't be decoded, shutting down gracefully", body)
        add_callback_threadsafe(state.connection, state.interrupt)


# https://stackoverflow.com/a/7099229/244258
def halt(
    decode: Decode,
    callback: ConsumerCallback,
    state: State[Any],
    channel: pika.channel.Channel,
    method: pika.spec.Basic.Deliver,
    properties: pika.BasicProperties,
    body: bytes,
) -> None:
    """
    If the ``callback`` function raises an exception, shut down the client in the main thread, without acknowledgment.
    """

    def errback(exception: Exception) -> None:
        logger.exception("Unhandled exception when consuming %r, shutting down gracefully", body)
        add_callback_threadsafe(state.connection, state.interrupt)

    decorate(decode, callback, state, channel, method, properties, body, errback)


def discard(
    decode: Decode,
    callback: ConsumerCallback,
    state: State[Any],
    channel: pika.channel.Channel,
    method: pika.spec.Basic.Deliver,
    properties: pika.BasicProperties,
    body: bytes,
) -> None:
    """
    If the ``callback`` function raises an exception, nack the message, without requeueing.
    """

    def errback(exception: Exception) -> None:
        logger.exception("Unhandled exception when consuming %r, discarding message", body)
        nack(state, channel, method.delivery_tag, requeue=False)

    decorate(decode, callback, state, channel, method, properties, body, errback)


def requeue(
    decode: Decode,
    callback: ConsumerCallback,
    state: State[Any],
    channel: pika.channel.Channel,
    method: pika.spec.Basic.Deliver,
    properties: pika.BasicProperties,
    body: bytes,
) -> None:
    """
    If the ``callback`` function raises an exception, nack the message, requeueing it unless it was redelivered.
    """

    def errback(exception: Exception) -> None:
        requeue = not method.redelivered
        logger.exception("Unhandled exception when consuming %r (requeue=%r)", body, requeue)
        nack(state, channel, method.delivery_tag, requeue=requeue)

    decorate(decode, callback, state, channel, method, properties, body, errback)
