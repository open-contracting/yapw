from __future__ import annotations

from typing import Any, Callable, NamedTuple, Optional

import pika

try:
    from typing import TypedDict
except ImportError:
    from typing_extensions import TypedDict  # type: ignore # https://github.com/python/mypy/issues/1153


#:
Encode = Callable[[Any, str], bytes]
#:
Decode = Callable[[bytes, Optional[str]], Any]


class State(NamedTuple):
    """
    Attributes that can be used safely in consumer callbacks.
    """

    #: A function to format the routing key.
    format_routing_key: Callable[[str], str]
    #: The connection.
    connection: pika.BlockingConnection
    #: The exchange name.
    exchange: str
    #: The message body's encoder.
    encode: Encode
    #: The message's content type.
    content_type: str
    #: The message's delivery mode.
    delivery_mode: int


class PublishKeywords(TypedDict, total=False):
    """
    Keyword arguments for ``basic_publish``.
    """

    #: The exchange to publish to.
    exchange: str
    #: The message's routing key.
    routing_key: str
    #: The message's body.
    body: bytes
    #: The message's content type and delivery mode.
    properties: pika.BasicProperties


#:
ConsumerCallback = Callable[[State, pika.channel.Channel, pika.spec.Basic.Deliver, pika.BasicProperties, Any], None]
#:
Decorator = Callable[
    [Decode, ConsumerCallback, State, pika.channel.Channel, pika.spec.Basic.Deliver, pika.BasicProperties, bytes], None
]
