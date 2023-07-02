import pika
import pytest

from yapw.clients import Base
from yapw.util import default_decode, default_encode


def dumps(message, content_type):
    return b"overridden"


def test_init_default():
    client = Base()

    expected = pika.URLParameters("amqp://127.0.0.1")
    expected.blocked_connection_timeout = 1800

    assert client.parameters == expected
    assert client.durable is True
    assert client.exchange == ""
    assert client.exchange_type == "direct"
    assert client.prefetch_count == 1
    assert client.decode == default_decode
    assert client.encode == default_encode
    assert client.content_type == "application/json"
    assert client.delivery_mode == 2
    assert client.routing_key_template == "{exchange}_{routing_key}"

    assert client.format_routing_key("test") == "_test"


def test_init_kwargs():
    client = Base(
        url="amqp://localhost",
        blocked_connection_timeout=0,
        durable=False,
        exchange="exch",
        exchange_type="fanout",
        prefetch_count=10,
        decode=default_decode,
        encode=dumps,
        content_type="application/octet-stream",
        routing_key_template="{routing_key}_{exchange}",
    )

    expected = pika.URLParameters("amqp://localhost")
    expected.blocked_connection_timeout = 0

    assert client.parameters == expected
    assert client.durable is False
    assert client.exchange == "exch"
    assert client.exchange_type == "fanout"
    assert client.prefetch_count == 10
    assert client.decode == default_decode
    assert client.encode == dumps
    assert client.content_type == "application/octet-stream"
    assert client.delivery_mode == 1
    assert client.routing_key_template == "{routing_key}_{exchange}"

    assert client.format_routing_key("test") == "test_exch"


def test_format_routing_key_invalid():
    client = Base(routing_key_template="{invalid}_{routing_key}")

    with pytest.raises(KeyError):
        client.format_routing_key("test")
