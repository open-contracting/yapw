"""
Decorators to be used with consumer callbacks.

A message must be ack'd or nack'd if using `consumer prefetch <https://www.rabbitmq.com/consumer-prefetch.html>`__,
because otherwise `RabbitMQ stops delivering messages <https://www.rabbitmq.com/confirms.html#channel-qos-prefetch>`__.
The decorators help to ensure that, in case of error, either the message is nack'd or the process is halted.

:func:`~yapw.decorators.halt` is the default decorator. For example, if a callback inserts messages into a database,
and the database is down, but this exception isn't handled by the callback, then the :func:`~yapw.decorators.discard`
or :func:`~yapw.decorators.requeue` decorators would end up nack'ing all messages in the queue. The ``halt`` decorator
instead stops the consumer, so that an administrator can decide when it is appropriate to restart it.

Decorators look like this:

.. code-block:: python

   def decorate(callback, state, channel, method, properties, body):
       try:
           callback(state, channel, method, properties, body)
       except Exception:
           # do something

User-defined decorators should avoid doing work outside the ``finally`` branch. Do work in the callback.
"""

import logging
import os
import signal

from yapw.methods import nack

logger = logging.getLogger(__name__)


def halt(callback, state, channel, method, properties, body):
    """
    If the callback raises an exception, send the SIGUSR1 signal to the main thread, without acknowledgment.
    """
    try:
        callback(state, channel, method, properties, body)
    except Exception:
        logger.exception("Unhandled exception when consuming %r, sending SIGUSR1", body)
        # https://stackoverflow.com/a/7099229/244258
        os.kill(os.getpid(), signal.SIGUSR1)


def discard(callback, state, channel, method, properties, body):
    """
    If the callback raises an exception, nack's the message without requeuing.
    """
    try:
        callback(state, channel, method, properties, body)
    except Exception:
        requeue = False
        logger.exception("Unhandled exception when consuming %r (requeue=%r)", body, requeue)
        nack(state, channel, method.delivery_tag, requeue=requeue)


def requeue(callback, state, channel, method, properties, body):
    """
    If the callback raises an exception, nack's the message, and requeues the message unless it was redelivered.
    """
    try:
        callback(state, channel, method, properties, body)
    except Exception:
        requeue = not method.redelivered
        logger.exception("Unhandled exception when consuming %r (requeue=%r)", body, requeue)
        nack(state, channel, method.delivery_tag, requeue=requeue)
