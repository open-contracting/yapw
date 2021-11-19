import logging
from unittest.mock import patch

import pika
import pytest

from yapw.clients import Base, Blocking, Durable, Transient


class DurableClient(Durable, Blocking, Base):
    pass


class TransientClient(Transient, Blocking, Base):
    pass


@pytest.mark.parametrize("client_class", [DurableClient, TransientClient])
@patch("pika.BlockingConnection")
def test_init_default(connection, client_class):
    client = client_class()

    client.channel.exchange_declare.assert_not_called()

    assert client.exchange == ""
    assert client.format_routing_key("test") == "_test"


@pytest.mark.parametrize("client_class,durable", [(DurableClient, True), (TransientClient, False)])
@patch("pika.BlockingConnection")
def test_init_kwargs(connection, client_class, durable):
    client = client_class(exchange="exch", exchange_type="fanout", routing_key_template="{routing_key}_{exchange}")

    client.channel.exchange_declare.assert_called_once_with(exchange="exch", exchange_type="fanout", durable=durable)

    assert client.exchange == "exch"
    assert client.format_routing_key("test") == "test_exch"


@pytest.mark.parametrize("client_class,durable", [(DurableClient, True), (TransientClient, False)])
@patch("pika.BlockingConnection")
def test_declare_queue(connection, client_class, durable):
    client = client_class(exchange="exch")

    client.declare_queue("q")

    client.channel.queue_declare.assert_called_once_with(queue="exch_q", durable=durable)
    client.channel.queue_bind.assert_called_once_with(exchange="exch", queue="exch_q", routing_key="exch_q")


@pytest.mark.parametrize("client_class,delivery_mode", [(DurableClient, 2), (TransientClient, 1)])
@patch("pika.BlockingConnection")
def test_publish(connection, client_class, delivery_mode, caplog):
    caplog.set_level(logging.DEBUG)

    client = client_class(exchange="exch")

    client.publish({"a": 1}, "q")

    properties = pika.BasicProperties(delivery_mode=delivery_mode, content_type="application/json")
    client.channel.basic_publish.assert_called_once_with(
        exchange="exch", routing_key="exch_q", body=b'{"a":1}', properties=properties
    )

    assert len(caplog.records) == 1
    assert caplog.records[-1].levelname == "DEBUG"
    assert caplog.records[-1].message == "Published message {'a': 1} to exchange exch with routing key exch_q"
