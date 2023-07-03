"""
Functions for calling channel methods from a consumer callback running in another thread.
"""

import functools
import logging
from typing import Any

import pika

from yapw.types import State
from yapw.util import basic_publish_debug_args, basic_publish_kwargs

logger = logging.getLogger(__name__)


def publish(
    state: State[Any], channel: pika.channel.Channel, message: Any, routing_key: str, *args: Any, **kwargs: Any
) -> None:
    """
    Publish the ``message`` with the ``routing_key``, to the exchange set by the ``state``.

    :param state: an object with thread-safe attributes from the client
    :param channel: the channel from which to call ``basic_publish``
    :param message: a decoded message
    :param routing_key: the routing key
    """
    keywords = basic_publish_kwargs(state, message, routing_key)
    keywords.update(kwargs)  # type: ignore # PEP 692 disallows updating TypedDict (PublishKeywords) with Dict.

    _channel_method_from_thread(state.connection, channel, "publish", *args, **keywords)
    logger.debug(*basic_publish_debug_args(channel, message, keywords))


def ack(state: State[Any], channel: pika.channel.Channel, delivery_tag: int | None = 0, **kwargs: bool) -> None:
    """
    Ack a message by its delivery tag.

    :param state: an object with thread-safe attributes from the client
    :param channel: the channel from which to call ``basic_ack``
    :param delivery_tag: the delivery tag
    """
    _channel_method_from_thread(state.connection, channel, "ack", delivery_tag, **kwargs)
    logger.debug("Ack'd message on channel %s with delivery tag %s", channel.channel_number, delivery_tag)


def nack(state: State[Any], channel: pika.channel.Channel, delivery_tag: int | None = 0, **kwargs: bool) -> None:
    """
    Nack a message by its delivery tag.

    :param state: an object with thread-safe attributes from the client
    :param channel: the channel from which to call ``basic_nack``
    :param delivery_tag: the delivery tag
    """
    _channel_method_from_thread(state.connection, channel, "nack", delivery_tag, **kwargs)
    logger.debug("Nack'd message on channel %s with delivery tag %s", channel.channel_number, delivery_tag)


def _channel_method_from_thread(
    connection: Any, channel: pika.channel.Channel, method: str, *args: Any, **kwargs: Any
) -> None:
    if connection.is_open:
        cb = functools.partial(_channel_method_from_main, channel, method, *args, **kwargs)
        add_callback_threadsafe(connection, cb)
    else:
        logger.error("Can't %s as connection is closed or closing", method)


def _channel_method_from_main(channel: pika.channel.Channel, method: str, *args: Any, **kwargs: Any) -> None:
    if channel.is_open:
        getattr(channel, f"basic_{method}")(*args, **kwargs)
    else:
        logger.error("Can't %s as channel is closed or closing", method)


def add_callback_threadsafe(connection: Any, callback: Any) -> None:
    """
    Interact with Pika from another thread.

    :param connection: a RabbitMQ connection
    :param callback: the callback to add
    """
    # One branch per adapter.
    if hasattr(connection, "ioloop"):
        connection.ioloop.call_soon_threadsafe(callback)
    else:
        connection.add_callback_threadsafe(callback)
