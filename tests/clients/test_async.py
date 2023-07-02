import logging
import os
from unittest.mock import patch

import pika
import pytest

from yapw.clients import Async

logger = logging.getLogger(__name__)

RABBIT_URL = os.getenv("TEST_RABBIT_URL", "amqp://127.0.0.1")


@patch("pika.SelectConnection")
def test_init_default(connection):
    Async().connect()

    connection.assert_called_once()

    assert connection.call_args[0][0].virtual_host == "/"
    assert connection.call_args[0][0].blocked_connection_timeout == 1800


@patch("pika.SelectConnection")
def test_init_kwargs(connection):
    Async(
        url="https://host:1234/%2Fv?blocked_connection_timeout=10",
        blocked_connection_timeout=300,
    ).connect()

    connection.assert_called_once()

    assert connection.call_args[0][0].virtual_host == "/v"
    assert connection.call_args[0][0].blocked_connection_timeout == 300


def test_connection_open_error(short_reconnect_delay, caplog):
    caplog.set_level(logging.CRITICAL, logger="pika")
    caplog.set_level(logging.DEBUG)

    client = Async(durable=False, url="amqp://nonexistent")
    # Prevent an infinite loop.
    client.stopping = True
    client.start()

    assert len(caplog.records) == 1
    assert [(r.levelname, r.message) for r in caplog.records] == [("ERROR", "Connection failed, retrying in 1s: ")]


def test_exchangeok_default(short_timer, caplog):
    caplog.set_level(logging.DEBUG)

    class Client(Async):
        def exchange_ready(self):
            logger.info("stop")

    client = Client(durable=False, url=RABBIT_URL)
    client.start()

    assert client.stopping is True

    assert len(caplog.records) == 3
    assert [(r.levelname, r.message) for r in caplog.records] == [
        ("INFO", "stop"),
        ("INFO", "Received SIGINT, shutting down gracefully"),
        ("WARNING", "Channel 1 was closed: (200, 'Normal shutdown')"),
    ]


@pytest.mark.parametrize("exchange_type", [pika.exchange_type.ExchangeType.direct, "direct"])
def test_exchangeok_kwargs(exchange_type, short_timer, caplog):
    caplog.set_level(logging.DEBUG)

    class Client(Async):
        def exchange_ready(self):
            pass

    client = Client(durable=False, url=RABBIT_URL, exchange="yapw_test", exchange_type=exchange_type)
    client.start()

    assert client.stopping is True

    assert len(caplog.records) == 3
    assert [(r.levelname, r.message) for r in caplog.records] == [
        ("DEBUG", "Declaring transient direct exchange yapw_test"),
        ("INFO", "Received SIGINT, shutting down gracefully"),
        ("WARNING", "Channel 1 was closed: (200, 'Normal shutdown')"),
    ]
