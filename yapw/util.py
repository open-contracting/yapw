import json

import pika

try:
    import orjson

    jsonlib = orjson
except ImportError:
    jsonlib = json


def json_dumps(data):
    """
    Serializes the data to JSON formatted bytes.
    """
    if jsonlib == json:
        return json.dumps(data, separators=(",", ":")).encode()
    return orjson.dumps(data)


def basic_publish_kwargs(state, message, routing_key):
    """
    Prepares keyword arguments for ``basic_publish``.

    :param state: an object with ``format_routing_key``, ``exchange`` and ``delivery_mode`` attributes
    :param message: a JSON-serializable message
    :param str routing_key: the routing key
    """
    formatted = state.format_routing_key(routing_key)

    body = json_dumps(message)
    properties = pika.BasicProperties(delivery_mode=state.delivery_mode, content_type="application/json")

    return {"exchange": state.exchange, "routing_key": formatted, "body": body, "properties": properties}


def basic_publish_debug_args(msg, kwargs):
    return "Published message %r to exchange %s with routing key %s", msg, kwargs["exchange"], kwargs["routing_key"]
