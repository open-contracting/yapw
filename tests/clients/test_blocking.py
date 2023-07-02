import logging
from unittest.mock import call, patch

import pika
import pytest

from yapw.clients import Blocking


@pytest.mark.parametrize("durable", [True, False])
@patch("pika.BlockingConnection")
def test_init_default(connection, durable):
    client = Blocking(durable=durable)

    connection.assert_called_once()

    assert connection.call_args[0][0].virtual_host == "/"
    assert connection.call_args[0][0].blocked_connection_timeout == 1800

    client.channel.basic_qos.assert_called_once_with(prefetch_count=1)
    client.channel.exchange_declare.assert_not_called()


@pytest.mark.parametrize("durable", [True, False])
@patch("pika.BlockingConnection")
def test_init_kwargs(connection, durable):
    client = Blocking(
        url="https://host:1234/%2Fv?blocked_connection_timeout=10",
        blocked_connection_timeout=300,
        durable=durable,
        exchange="exch",
        exchange_type="fanout",
        prefetch_count=10,
    )

    connection.assert_called_once()

    assert connection.call_args[0][0].virtual_host == "/v"
    assert connection.call_args[0][0].blocked_connection_timeout == 300

    client.channel.basic_qos.assert_called_once_with(prefetch_count=10)
    client.channel.exchange_declare.assert_called_once_with(exchange="exch", exchange_type="fanout", durable=durable)


@pytest.mark.parametrize("durable", [True, False])
@patch("pika.BlockingConnection")
def test_declare_queue(connection, durable):
    client = Blocking(durable=durable, exchange="exch")

    client.declare_queue("q")

    client.channel.queue_declare.assert_called_once_with(queue="exch_q", durable=durable, arguments=None)
    assert client.channel.queue_bind.call_count == 1
    client.channel.queue_bind.assert_has_calls(
        [
            call(exchange="exch", queue="exch_q", routing_key="exch_q"),
        ]
    )


@pytest.mark.parametrize("durable", [True, False])
@patch("pika.BlockingConnection")
def test_declare_queue_routing_keys(connection, durable):
    client = Blocking(durable=durable, exchange="exch")

    client.declare_queue("q", ["r", "k"])

    client.channel.queue_declare.assert_called_once_with(queue="exch_q", durable=durable, arguments=None)
    assert client.channel.queue_bind.call_count == 2
    client.channel.queue_bind.assert_has_calls(
        [
            call(exchange="exch", queue="exch_q", routing_key="exch_r"),
            call(exchange="exch", queue="exch_q", routing_key="exch_k"),
        ]
    )


@pytest.mark.parametrize("durable,delivery_mode", [(True, 2), (False, 1)])
@patch("pika.BlockingConnection")
def test_publish(connection, durable, delivery_mode, caplog):
    connection.return_value.channel.return_value.channel_number = 1

    caplog.set_level(logging.DEBUG)

    client = Blocking(durable=durable, exchange="exch")

    client.publish({"a": 1}, "q")

    properties = pika.BasicProperties(delivery_mode=delivery_mode, content_type="application/json")
    client.channel.basic_publish.assert_called_once_with(
        exchange="exch", routing_key="exch_q", body=b'{"a":1}', properties=properties
    )

    assert len(caplog.records) == 1
    record = caplog.records[-1]
    assert record.levelname == "DEBUG"
    assert record.message == "Published message {'a': 1} on channel 1 to exchange exch with routing key exch_q"
