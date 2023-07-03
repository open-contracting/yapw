import logging
import os
import sys
from unittest.mock import patch

import pika
import pytest

from yapw.clients import Async

logger = logging.getLogger(__name__)

RABBIT_URL = os.getenv("TEST_RABBIT_URL", "amqp://127.0.0.1")


@patch("yapw.clients.AsyncioConnection")
def test_init_default(connection):
    Async().connect()

    connection.assert_called_once()

    assert connection.call_args[0][0].virtual_host == "/"
    assert connection.call_args[0][0].blocked_connection_timeout == 1800


@patch("yapw.clients.AsyncioConnection")
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
    caplog.set_level(logging.INFO, logger="asyncio")
    caplog.set_level(logging.DEBUG)

    client = Async(durable=False, url="amqp://nonexistent")
    # Prevent an infinite loop.
    client.stopping = True
    client.start()

    # Channel never opened.
    assert client.connection.is_closed

    assert len(caplog.records) == 1
    assert [(r.levelname, r.message) for r in caplog.records] == [("ERROR", "Connection failed, retrying in 1s: ")]


def test_connection_close(short_reconnect_delay, caplog):
    class Client(Async):
        def __init__(self, **kwargs):
            super().__init__(**kwargs)
            self.overridden = False

        def exchange_ready(self):
            self.interrupt()
            if not self.overridden:  # use the second branch of connection_close_callback() once
                self.stopping = False
                self.overridden = True

    client = Client(durable=False, url=RABBIT_URL)
    client.start()

    assert client.channel.is_closed
    assert client.connection.is_closed

    assert len(caplog.records) == 3
    assert [(r.levelname, r.message) for r in caplog.records] == [
        ("WARNING", "Channel 1 was closed: (200, 'Normal shutdown')"),
        ("ERROR", "Connection closed, reconnecting in 1s: (200, 'Normal shutdown')"),
        ("WARNING", "Channel 1 was closed: (200, 'Normal shutdown')"),
    ]


def test_exchangeok_default(short_timer, caplog):
    caplog.set_level(logging.DEBUG)

    class Client(Async):
        def exchange_ready(self):
            logger.info("stop")

    client = Client(durable=False, url=RABBIT_URL)
    client.start()

    assert client.channel.is_closed
    assert client.connection.is_closed

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

    if exchange_type is pika.exchange_type.ExchangeType.direct and sys.version_info < (3, 11):
        infix = "ExchangeType.direct"
    else:
        infix = "direct"

    assert client.channel.is_closed
    assert client.connection.is_closed

    assert len(caplog.records) == 3
    assert [(r.levelname, r.message) for r in caplog.records] == [
        ("DEBUG", f"Declaring transient {infix} exchange yapw_test"),
        ("INFO", "Received SIGINT, shutting down gracefully"),
        ("WARNING", "Channel 1 was closed: (200, 'Normal shutdown')"),
    ]
