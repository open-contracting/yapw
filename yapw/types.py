import sys
from collections.abc import Callable
from typing import Any, Generic, NamedTuple, Optional, TypedDict, TypeVar

import pika

T = TypeVar("T")
Encode = Callable[[Any, str], bytes]
Decode = Callable[[bytes, Optional[str]], Any]


if sys.version_info >= (3, 11):

    class State(NamedTuple, Generic[T]):
        """
        Attributes that can be used safely in consumer callbacks.
        """

        #: A function to format the routing key.
        format_routing_key: Callable[[str], str]
        #: A function to shut down the client.
        interrupt: Callable[[T], None]
        #: The connection.
        connection: T
        #: The exchange name.
        exchange: str
        #: The message bodies' encoder.
        encode: Encode
        #: The messages' content type.
        content_type: str
        #: The messages' delivery mode.
        delivery_mode: int

else:

    class State(NamedTuple):
        """
        Attributes that can be used safely in consumer callbacks.
        """

        #: A function to format the routing key.
        format_routing_key: Callable[[str], str]
        #: A function to shut down the client.
        interrupt: Callable[[T], None]
        #: The connection.
        connection: T
        #: The exchange name.
        exchange: str
        #: The message bodies' encoder.
        encode: Encode
        #: The messages' content type.
        content_type: str
        #: The messages' delivery mode.
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


ConsumerCallback = Callable[[State, pika.channel.Channel, pika.spec.Basic.Deliver, pika.BasicProperties, Any], None]
Decorator = Callable[
    [Decode, ConsumerCallback, State, pika.channel.Channel, pika.spec.Basic.Deliver, pika.BasicProperties, bytes], None
]
