import json

import pika

try:
    import orjson

    jsonlib = orjson
except ImportError:
    jsonlib = json


def json_dumps(message):
    """
    Serializes the message to JSON formatted bytes.

    Uses `orjson <https://pypi.org/project/orjson/>`__ if available.

    :param message: a JSON-serializable message
    :returns: JSON formatted bytes
    :rtype: bytes
    """
    if jsonlib == json:
        return json.dumps(message, separators=(",", ":")).encode()
    return orjson.dumps(message)


def basic_publish_kwargs(state, message, routing_key):
    """
    Prepares keyword arguments for ``basic_publish``.

    :param state: an object with the attributes ``format_routing_key``, ``exchange``, ``encoder``, ``content_type`` and
                  ``delivery_mode``
    :param message: a JSON-serializable message
    :param str routing_key: the routing key
    :returns: keyword arguments for ``basic_publish``
    :rtype: dict
    """
    formatted = state.format_routing_key(routing_key)

    body = state.encoder(message)
    properties = pika.BasicProperties(delivery_mode=state.delivery_mode, content_type=state.content_type)

    return {"exchange": state.exchange, "routing_key": formatted, "body": body, "properties": properties}


def basic_publish_debug_args(channel, message, keywords):
    """
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
