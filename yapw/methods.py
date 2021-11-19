"""
Functions for calling RabbitMQ methods from the context of a consumer callback.
"""

import functools
import logging

logger = logging.getLogger(__name__)


def publish(connection, channel, *args, **kwargs):
    """
    Publish a message.
    """
    _channel_method_from_thread(connection, channel, "publish", *args, **kwargs)


def ack(connection, channel, *args, **kwargs):
    """
    ACK a message.
    """
    _channel_method_from_thread(connection, channel, "ack", *args, **kwargs)


def nack(connection, channel, *args, **kwargs):
    """
    NACK a message.
    """
    _channel_method_from_thread(connection, channel, "nack", *args, **kwargs)


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
