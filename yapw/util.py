import json

import pika

try:
    import orjson

    jsonlib = orjson
except ImportError:
    jsonlib = json


def json_dumps(message):
    """
    Serializes a Python object to JSON formatted bytes.

    Uses `orjson <https://pypi.org/project/orjson/>`__ if available.

    :param message: a Python object
    :returns: JSON formatted bytes
    :rtype: bytes
    """
    if jsonlib == json:
        return json.dumps(message, separators=(",", ":")).encode()
    return orjson.dumps(message)


def default_encode(message, content_type):
    """
    If the content type is "application/json", serializes the decoded message to JSON formatted bytes. Otherwise,
    returns the decoded message (which might be bytes already).

    :param message: a decoded message
    :param str content_type: the message's content type
    :returns: bytes
    :rtype: bytes
    """
    if content_type == "application/json":
        return json_dumps(message)
    return message


def basic_publish_kwargs(state, message, routing_key):
    """
    Prepares keyword arguments for ``basic_publish``.

    :param state: an object with the attributes ``format_routing_key``, ``exchange``, ``encode``, ``content_type`` and
                  ``delivery_mode``
    :param message: a decoded message
    :param str routing_key: the routing key
    :returns: keyword arguments for ``basic_publish``
    :rtype: dict
    """
    formatted = state.format_routing_key(routing_key)

    body = state.encode(message, state.content_type)
    properties = pika.BasicProperties(content_type=state.content_type, delivery_mode=state.delivery_mode)

    return {"exchange": state.exchange, "routing_key": formatted, "body": body, "properties": properties}


def basic_publish_debug_args(channel, message, keywords):
    """
    Prepares arguments for ``logger.debug`` related to publishing a message.

    :param channel: the channel from which to call ``basic_publish``
    :param message: a decoded message
    :param dict keywords: keyword arguments for ``basic_publish``
    :returns: arguments for ``logger.debug``
    :rtype: tuple
    """
    return (
        "Published message %r on channel %s to exchange %s with routing key %s",
        message,
        channel.channel_number,
        keywords["exchange"],
        keywords["routing_key"],
    )
