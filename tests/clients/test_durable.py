from unittest.mock import patch

import pika

from yapw.clients import Base, Blocking, Durable


class Client(Durable, Blocking, Base):
    pass


@patch("pika.BlockingConnection")
def test_init_default(connection):
    client = Client()

    client.channel.exchange_declare.assert_not_called()

    assert client.exchange == ""
    assert client.format_routing_key("test") == "_test"


@patch("pika.BlockingConnection")
def test_init_kwargs(connection):
    client = Client(exchange="exch", exchange_type="fanout", routing_key_template="{routing_key}_{exchange}")

    client.channel.exchange_declare.assert_called_once_with(exchange="exch", exchange_type="fanout", durable=True)

    assert client.exchange == "exch"
    assert client.format_routing_key("test") == "test_exch"


@patch("pika.BlockingConnection")
def test_declare_queue(connection):
    client = Client(exchange="exch")

    client.declare_queue("q")

    client.channel.queue_declare.assert_called_once_with(queue="exch_q", durable=True)
    client.channel.queue_bind.assert_called_once_with(exchange="exch", queue="exch_q", routing_key="exch_q")


@patch("pika.BlockingConnection")
def test_publish(connection):
    client = Client(exchange="exch")

    client.publish({"a": 1}, "q")

    properties = pika.BasicProperties(delivery_mode=2, content_type="application/json")
    client.channel.basic_publish.assert_called_once_with(
        exchange="exch", routing_key="exch_q", body=b'{"a":1}', properties=properties
    )
