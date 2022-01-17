import logging
from unittest.mock import call, patch

import pika
import pytest

from yapw.clients import Base, Blocking, Durable, Transient
from yapw.util import default_encode


class DurableClient(Durable, Blocking, Base):
    pass


class TransientClient(Transient, Blocking, Base):
    pass


def dumps(message, content_type):
    return b"overridden"


@pytest.mark.parametrize("client_class", [DurableClient, TransientClient])
@patch("pika.BlockingConnection")
def test_init_default(connection, client_class):
    client = client_class()

    client.channel.exchange_declare.assert_not_called()

    assert client.exchange == ""
    assert client.encode == default_encode
    assert client.content_type == "application/json"
    assert client.format_routing_key("test") == "_test"


@pytest.mark.parametrize("client_class,durable", [(DurableClient, True), (TransientClient, False)])
@patch("pika.BlockingConnection")
def test_init_kwargs(connection, client_class, durable):
    client = client_class(
        exchange="exch",
        exchange_type="fanout",
        encode=dumps,
        content_type="application/octet-stream",
        routing_key_template="{routing_key}_{exchange}",
    )

    client.channel.exchange_declare.assert_called_once_with(exchange="exch", exchange_type="fanout", durable=durable)

    assert client.exchange == "exch"
    assert client.encode == dumps
    assert client.content_type == "application/octet-stream"
    assert client.format_routing_key("test") == "test_exch"


@pytest.mark.parametrize("client_class,durable", [(DurableClient, True), (TransientClient, False)])
@patch("pika.BlockingConnection")
def test_declare_queue(connection, client_class, durable):
    client = client_class(exchange="exch")

    client.declare_queue("q")

    client.channel.queue_declare.assert_called_once_with(queue="exch_q", durable=durable)
    assert client.channel.queue_bind.call_count == 1
    client.channel.queue_bind.assert_has_calls(
        [
            call(exchange="exch", queue="exch_q", routing_key="exch_q"),
        ]
    )


@pytest.mark.parametrize("client_class,durable", [(DurableClient, True), (TransientClient, False)])
@patch("pika.BlockingConnection")
def test_declare_queue_routing_keys(connection, client_class, durable):
    client = client_class(exchange="exch")

    client.declare_queue("q", ["r", "k"])

    client.channel.queue_declare.assert_called_once_with(queue="exch_q", durable=durable)
    assert client.channel.queue_bind.call_count == 2
    client.channel.queue_bind.assert_has_calls(
        [
            call(exchange="exch", queue="exch_q", routing_key="exch_r"),
            call(exchange="exch", queue="exch_q", routing_key="exch_k"),
        ]
    )


@pytest.mark.parametrize("client_class,delivery_mode", [(DurableClient, 2), (TransientClient, 1)])
@patch("pika.BlockingConnection")
def test_publish(connection, client_class, delivery_mode, caplog):
    connection.return_value.channel.return_value.channel_number = 1

    caplog.set_level(logging.DEBUG)

    client = client_class(exchange="exch")

    client.publish({"a": 1}, "q")

    properties = pika.BasicProperties(delivery_mode=delivery_mode, content_type="application/json")
    client.channel.basic_publish.assert_called_once_with(
        exchange="exch", routing_key="exch_q", body=b'{"a":1}', properties=properties
    )

    assert len(caplog.records) == 1
    record = caplog.records[-1]
    assert record.levelname == "DEBUG"
    assert record.message == "Published message {'a': 1} on channel 1 to exchange exch with routing key exch_q"
