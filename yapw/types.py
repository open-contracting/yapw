import asyncio
from collections.abc import Callable
from typing import Any, Generic, NamedTuple, TypedDict, TypeVar

import pika
from pika.exchange_type import ExchangeType

T = TypeVar("T")
Encode = Callable[[Any, str], bytes]
Decode = Callable[[bytes, str | None], Any]


class State(NamedTuple, Generic[T]):
    """Attributes that can be used safely in consumer callbacks."""

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


class PublishOptions(TypedDict, total=False):
    """Keyword arguments for :func:`yapw.methods.publish`."""

    #: The exchange to publish to.
    exchange: str
    #: The message's content type and delivery mode.
    properties: pika.BasicProperties
    #: Whether the message must be routed to a queue.
    mandatory: bool


class PublishKeywords(PublishOptions, total=False):
    """Keyword arguments for ``basic_publish``."""

    #: The message's routing key.
    routing_key: str
    #: The message's body.
    body: bytes


class AckKeywords(TypedDict, total=False):
    """Keyword arguments for ``basic_ack``."""

    #: Whether to ack all messages up to and including the delivery tag.
    multiple: bool


class NackKeywords(AckKeywords, total=False):
    """Keyword arguments for ``basic_nack``."""

    #: Whether to requeue the message(s).
    requeue: bool


class ClientKeywords(TypedDict, total=False):
    """Keyword arguments for :class:`yapw.clients.Base`."""

    url: str
    blocked_connection_timeout: float
    durable: bool
    exchange: str
    exchange_type: ExchangeType
    prefetch_count: int
    decode: Decode
    encode: Encode
    content_type: str
    routing_key_template: str


class AsyncKeywords(ClientKeywords, total=False):
    """Keyword arguments for :class:`yapw.clients.Async`."""

    custom_ioloop: asyncio.AbstractEventLoop | None
    manage_ioloop: bool


ConsumerCallback = Callable[
    [State[Any], pika.channel.Channel, pika.spec.Basic.Deliver, pika.BasicProperties, Any],
    None,
]
Decorator = Callable[
    [Decode, ConsumerCallback, State[Any], pika.channel.Channel, pika.spec.Basic.Deliver, pika.BasicProperties, bytes],
    None,
]
