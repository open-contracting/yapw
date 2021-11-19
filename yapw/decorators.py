"""
Decorators to be used with consumer callbacks.

A message must be ack'd or nack'd if using `consumer prefetch <https://www.rabbitmq.com/consumer-prefetch.html>`__,
because otherwise `RabbitMQ stops delivering messages <https://www.rabbitmq.com/confirms.html#channel-qos-prefetch>`__.
The decorators help to ensure that the message is nack'd in case of errors.
"""

import logging

from yapw.methods import nack

logger = logging.getLogger(__name__)


def rescue(callback, state, channel, method, properties, body):
    """
    If the callback raises an exception, nack's the message without requeuing.
    """
    delivery_tag = method.delivery_tag
    try:
        callback(state, channel, method, properties, body)
    except Exception:
        requeue = False
        logger.exception("Unhandled exception when consuming %r (requeue=%r)", body, requeue)
        nack(state, channel, delivery_tag=delivery_tag, requeue=requeue)


def requeue(callback, state, channel, method, properties, body):
    """
    If the callback raises an exception, nack's the message, and requeues the message unless it was redelivered.
    """
    delivery_tag = method.delivery_tag
    try:
        callback(state, channel, method, properties, body)
    except Exception:
        requeue = not method.redelivered
        logger.exception("Unhandled exception when consuming %r (requeue=%r)", body, requeue)
        nack(state, channel, delivery_tag=delivery_tag, requeue=requeue)
