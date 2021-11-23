import json
import pickle
from typing import TYPE_CHECKING, Any, Tuple, Union

import pika

try:
    import orjson

    jsonlib = orjson
except ImportError:
    jsonlib = json  # type: ignore # https://github.com/python/mypy/issues/1153

from yapw.types import PublishKeywords, State

if TYPE_CHECKING:
    from yapw.clients import Publisher


def json_dumps(message: Any) -> bytes:
    """
    Serialize a Python object to JSON formatted bytes.

    Uses `orjson <https://pypi.org/project/orjson/>`__ if available.

    :param message: a Python object
    :returns: JSON formatted bytes
    """
    if jsonlib == json:
        return json.dumps(message, separators=(",", ":")).encode()
    return orjson.dumps(message)


def default_encode(message: Any, content_type: str) -> bytes:
    """
    Encode the decoded message to bytes.

    -  If the content type is "application/json", serialize the message to JSON formatted bytes.
    -  If the message is a string, encode it to bytes.
    -  If the message is bytes, return it.
    -  Otherwise, serialize the message to its picked representation.

    :param message: a decoded message
    :param content_type: the message's content type
    :returns: an encoded message
    """
    if content_type == "application/json":
        return json_dumps(message)
    if isinstance(message, str):
        return message.encode()
    if isinstance(message, bytes):
        return message
    return pickle.dumps(message)


def basic_publish_kwargs(state: Union["Publisher", State], message: Any, routing_key: str) -> PublishKeywords:
    """
    Prepare keyword arguments for ``basic_publish``.

    :param state: an object with the attributes ``format_routing_key``, ``exchange``, ``encode``, ``content_type`` and
                  ``delivery_mode``
    :param message: a decoded message
    :param routing_key: the routing key
    :returns: keyword arguments for ``basic_publish``
    """
    formatted = state.format_routing_key(routing_key)

    body = state.encode(message, state.content_type)
    properties = pika.BasicProperties(content_type=state.content_type, delivery_mode=state.delivery_mode)

    return {"exchange": state.exchange, "routing_key": formatted, "body": body, "properties": properties}


def basic_publish_debug_args(
    channel: pika.channel.Channel, message: Any, keywords: PublishKeywords
) -> Tuple[str, Any, int, str, str]:
    """
    Prepare arguments for ``logger.debug`` related to publishing a message.

    :param channel: the channel from which to call ``basic_publish``
    :param message: a decoded message
    :param keywords: keyword arguments for ``basic_publish``
    :returns: arguments for ``logger.debug``
    """
    return (
        "Published message %r on channel %s to exchange %s with routing key %s",
        message,
        channel.channel_number,
        keywords["exchange"],
        keywords["routing_key"],
    )
