"""
Functions for calling channel methods from the context of a consumer callback.
"""

import functools
import logging
from typing import Any, Tuple

import pika

from yapw.util import basic_publish_debug_args, basic_publish_kwargs

logger = logging.getLogger(__name__)


def publish(state: Tuple, channel: pika.channel.Channel, message: Any, routing_key: str, *args, **kwargs) -> None:
    """
    Publish with the provided message and routing key, and with the exchange set by the provided state.

    :param state: an object with a ``connection`` attribute
    :param channel: the channel from which to call ``basic_publish``
    :param message: a decoded message
    :param routing_key: the routing key
    """
    keywords = basic_publish_kwargs(state, message, routing_key)
    keywords.update(kwargs)

    _channel_method_from_thread(state.connection, channel, "publish", *args, **keywords)
    logger.debug(*basic_publish_debug_args(channel, message, keywords))


def ack(state: Tuple, channel: pika.channel.Channel, delivery_tag: int = 0, **kwargs) -> None:
    """
    Ack a message by its delivery tag.

    :param state: an object with a ``connection`` attribute
    :param channel: the channel from which to call ``basic_ack``
    :param delivery_tag: the delivery tag
    """
    _channel_method_from_thread(state.connection, channel, "ack", delivery_tag, **kwargs)
    logger.debug("Ack'd message on channel %s with delivery tag %s", channel.channel_number, delivery_tag)


def nack(state: Tuple, channel: pika.channel.Channel, delivery_tag: int = 0, **kwargs) -> None:
    """
    Nack a message by its delivery tag.

    :param state: an object with a ``connection`` attribute
    :param channel: the channel from which to call ``basic_nack``
    :param delivery_tag: the delivery tag
    """
    _channel_method_from_thread(state.connection, channel, "nack", delivery_tag, **kwargs)
    logger.debug("Nack'd message on channel %s with delivery tag %s", channel.channel_number, delivery_tag)


def _channel_method_from_thread(
    connection: pika.connection.Connection, channel: pika.channel.Channel, method: str, *args, **kwargs
) -> None:
    if connection.is_open:
        cb = functools.partial(_channel_method_from_main, channel, method, *args, **kwargs)
        connection.add_callback_threadsafe(cb)
    else:
        logger.error("Can't %s as connection is closed or closing", method)


def _channel_method_from_main(channel: pika.channel.Channel, method: str, *args, **kwargs) -> None:
    if channel.is_open:
        getattr(channel, f"basic_{method}")(*args, **kwargs)
    else:
        logger.error("Can't %s as channel is closed or closing", method)
