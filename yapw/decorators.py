"""
Decorators to be used with consumer callbacks.

A message must be ack'd or nack'd if using `consumer prefetch <https://www.rabbitmq.com/consumer-prefetch.html>`__,
because otherwise `RabbitMQ stops delivering messages <https://www.rabbitmq.com/confirms.html#channel-qos-prefetch>`__.
The decorators help to ensure that, in case of error, either the message is nack'd or the process is halted.

:func:`~yapw.decorators.halt` is the default decorator. For example, if a callback inserts messages into a database,
and the database is down, but this exception isn't handled by the callback, then the :func:`~yapw.decorators.discard`
or :func:`~yapw.decorators.requeue` decorators would end up nack'ing all messages in the queue. The ``halt`` decorator
instead stops the consumer, so that an administrator can decide when it is appropriate to restart it.

Decorators look like this (see :func:`~yapw.decorators.decorate` for context):

.. code-block:: python

   from yapw.decorators import decorate


   def myfunction(decode, callback, state, channel, method, properties, body):
       def errback():
           # do something, like halting the process or nack'ing the message

       decorate(decode, callback, state, channel, method, properties, body, errback)

User-defined decorators should avoid doing work outside the ``finally`` branch. Do work in the callback.
"""
from __future__ import annotations

import logging
import os
import signal
from typing import Any, Callable, Optional

import pika

from yapw.methods.blocking import nack
from yapw.types import ConsumerCallback, Decode, State
from yapw.util import jsonlib

logger = logging.getLogger(__name__)


def default_decode(body: bytes, content_type: Optional[str]) -> Any:
    """
    If the content type is "application/json", deserializes the JSON formatted bytes to a Python object. Otherwise,
    returns the bytes (which the consumer callback can deserialize independently).

    Uses `orjson <https://pypi.org/project/orjson/>`__ if available.

    :param body: the encoded message
    :param content_type: the message's content type
    :returns: a Python object
    """
    if content_type == "application/json":
        return jsonlib.loads(body)
    return body


def decorate(
    decode: Decode,
    callback: ConsumerCallback,
    state: State,
    channel: pika.channel.Channel,
    method: pika.spec.Basic.Deliver,
    properties: pika.BasicProperties,
    body: bytes,
    errback: Callable[[], None],
    finalback: Optional[Callable[[], None]] = None,
) -> None:
    """
    Decode the message ``body`` using the ``decode`` function, and call the consumer ``callback``.

    If the ``callback`` function raises an exception, call the ``errback`` function.

    If the ``decode`` function raises an exception, send the SIGUSR2 signal to the main thread.

    .. seealso::

       :meth:`yapw.clients.Threaded.consume` for details on the consumer callback function signature.
    """
    logger.debug(
        "Received message %s with routing key %s and delivery tag %s", body, method.routing_key, method.delivery_tag
    )
    try:
        message = decode(body, properties.content_type)
        try:
            callback(state, channel, method, properties, message)
        except Exception:
            errback()
        finally:
            if finalback:
                finalback()
    except Exception:
        logger.exception("%r can't be decoded, sending SIGUSR2", body)
        os.kill(os.getpid(), signal.SIGUSR2)


# https://stackoverflow.com/a/7099229/244258
def halt(
    decode: Decode,
    callback: ConsumerCallback,
    state: State,
    channel: pika.channel.Channel,
    method: pika.spec.Basic.Deliver,
    properties: pika.BasicProperties,
    body: bytes,
) -> None:
    """
    If the callback raises an exception, send the SIGUSR1 signal to the main thread, without acknowledgment.
    """

    def errback() -> None:
        logger.exception("Unhandled exception when consuming %r, sending SIGUSR1", body)
        os.kill(os.getpid(), signal.SIGUSR1)

    decorate(decode, callback, state, channel, method, properties, body, errback)


def discard(
    decode: Decode,
    callback: ConsumerCallback,
    state: State,
    channel: pika.channel.Channel,
    method: pika.spec.Basic.Deliver,
    properties: pika.BasicProperties,
    body: bytes,
) -> None:
    """
    If the callback raises an exception, nack the message without requeueing.
    """

    def errback() -> None:
        logger.exception("Unhandled exception when consuming %r, discarding message", body)
        nack(state, channel, method.delivery_tag, requeue=False)

    decorate(decode, callback, state, channel, method, properties, body, errback)


def requeue(
    decode: Decode,
    callback: ConsumerCallback,
    state: State,
    channel: pika.channel.Channel,
    method: pika.spec.Basic.Deliver,
    properties: pika.BasicProperties,
    body: bytes,
) -> None:
    """
    If the callback raises an exception, nack the message, and requeue the message unless it was redelivered.
    """

    def errback() -> None:
        requeue = not method.redelivered
        logger.exception("Unhandled exception when consuming %r (requeue=%r)", body, requeue)
        nack(state, channel, method.delivery_tag, requeue=requeue)

    decorate(decode, callback, state, channel, method, properties, body, errback)
