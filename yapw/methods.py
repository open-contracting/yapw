"""
Defines functions for calling RabbitMQ methods from the context of a consumer callback.
"""

import functools
import logging

logger = logging.getLogger(__name__)


def ack(connection, channel, *args, **kwargs):
    """
    ACK a message.
    """
    if connection.is_open:
        cb = functools.partial(_ack_message, channel, *args, **kwargs)
        connection.add_callback_threadsafe(cb)
    else:
        logger.error("Can't ACK as connection is closed or closing")


def nack(connection, channel, *args, **kwargs):
    """
    NACK a message.
    """
    if connection.is_open:
        cb = functools.partial(_nack_message, channel, *args, **kwargs)
        connection.add_callback_threadsafe(cb)
    else:
        logger.error("Can't NACK as connection is closed or closing")


def _ack_message(channel, *args, **kwargs):
    if channel.is_open:
        channel.basic_ack(*args, **kwargs)
    else:
        logger.error("Can't ACK as channel is closed or closing")


def _nack_message(channel, *args, **kwargs):
    if channel.is_open:
        channel.basic_nack(*args, **kwargs)
    else:
        logger.error("Can't NACK as channel is closed or closing")
