"""
Functions for calling RabbitMQ methods from the context of a consumer callback.
"""

import functools
import logging

from yapw.util import basic_publish_kwargs

logger = logging.getLogger(__name__)


def publish(state, channel, message, routing_key, *args, **kwargs):
    """
    Publishes with the provided message and routing key, and with the exchange set by the provided state.

    :param state: an object with a ``connection`` attribute
    :param channel: the channel from which to call ``basic_publish``
    :param message: a JSON-serializable message
    :param str routing_key: the routing key
    """
    keywords = basic_publish_kwargs(state, message, routing_key)
    keywords.update(kwargs)

    _channel_method_from_thread(state.connection, channel, "publish", *args, **keywords)
    logger.debug(
        "Published message %r to exchange %s with routing key %s",
        message,
        keywords["exchange"],
        keywords["routing_key"],
    )


def ack(state, channel, *args, **kwargs):
    """
    Acks a message by its delivery tag.

    :param state: an object with a ``connection`` attribute
    :param channel: the channel from which to call ``basic_ack``
    """
    _channel_method_from_thread(state.connection, channel, "ack", *args, **kwargs)


def nack(state, channel, *args, **kwargs):
    """
    Nacks a message by its delivery tag.

    :param state: an object with a ``connection`` attribute
    :param channel: the channel from which to call ``basic_nack``
    """
    _channel_method_from_thread(state.connection, channel, "nack", *args, **kwargs)


def _channel_method_from_thread(connection, channel, method, *args, **kwargs):
    if connection.is_open:
        cb = functools.partial(_channel_method_from_main, channel, method, *args, **kwargs)
        connection.add_callback_threadsafe(cb)
    else:
        logger.error("Can't %s as connection is closed or closing", method)


def _channel_method_from_main(channel, method, *args, **kwargs):
    if channel.is_open:
        getattr(channel, f"basic_{method}")(*args, **kwargs)
    else:
        logger.error("Can't %s as channel is closed or closing", method)
