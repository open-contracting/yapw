import logging
import os
import re
import sys
from unittest.mock import patch
from urllib.parse import urlsplit

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


@pytest.mark.parametrize(
    "url,expected",
    [
        (
            "amqp://invalid",
            r"AMQPConnectionWorkflowFailed: 1 exceptions in all; last exception - gaierror\(",
        ),
        (
            "amqp://127.0.0.1:1024",
            r"AMQPConnectionError: \(AMQPConnectionWorkflowFailed: 1 exceptions in all; last exception - "
            r"AMQPConnectorSocketConnectError: ConnectionRefusedError\(",
        ),
    ],
)
def test_connection_open_error_bad_host_or_port(url, expected, short_reconnect_delay, caplog):
    caplog.set_level(logging.CRITICAL, logger="pika")
    caplog.set_level(logging.INFO, logger="asyncio")
    caplog.set_level(logging.DEBUG)

    client = Async(durable=False, url=url)
    # Prevent an infinite loop.
    client.stopping = True
    client.start()

    # Channel never opened.
    assert client.connection.is_closed

    assert len(caplog.records) == 1
    for r in caplog.records:
        assert r.levelname == "ERROR"
        assert re.search(r"^Connection failed, retrying in 1s: " + expected, r.message)


@pytest.mark.parametrize("auth", ["invalid:invalid", "guest:invalid"])
def test_connection_open_error_bad_username_or_password(auth, short_reconnect_delay, caplog):
    caplog.set_level(logging.CRITICAL, logger="pika")
    caplog.set_level(logging.INFO, logger="asyncio")
    caplog.set_level(logging.DEBUG)

    parsed = urlsplit(RABBIT_URL)
    parsed = parsed._replace(netloc=f"{auth}@{parsed.hostname}:{parsed.port or 5672}")

    client = Async(durable=False, url=parsed.geturl())
    # Prevent an infinite loop.
    client.stopping = True
    client.start()

    # Channel never opened.
    assert client.connection.is_closed

    assert len(caplog.records) == 1
    assert [(r.levelname, r.message) for r in caplog.records] == [
        (
            "ERROR",
            "Stopping: ProbableAuthenticationError: Client was disconnected at a connection stage indicating a "
            "probable authentication error: (\"ConnectionClosedByBroker: (403) 'ACCESS_REFUSED - Login was refused "
            "using authentication mechanism PLAIN. For details see the broker logfile.'\",)",
        )
    ]


def test_connection_open_error_bad_virtual_host(short_reconnect_delay, caplog):
    caplog.set_level(logging.CRITICAL, logger="pika")
    caplog.set_level(logging.INFO, logger="asyncio")
    caplog.set_level(logging.DEBUG)

    parsed = urlsplit(RABBIT_URL)
    parsed = parsed._replace(path="/%2Finvalid")

    client = Async(durable=False, url=parsed.geturl())
    # Prevent an infinite loop.
    client.stopping = True
    client.start()

    # Channel never opened.
    assert client.connection.is_closed

    assert len(caplog.records) == 1
    assert [(r.levelname, r.message) for r in caplog.records] == [
        (
            "ERROR",
            "Stopping: ProbableAccessDeniedError: Client was disconnected at a connection stage indicating a probable "
            "denial of access to the specified virtual host: (\"ConnectionClosedByBroker: (530) 'NOT_ALLOWED - vhost "
            "/invalid not found'\",)",
        )
    ]


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
        ("WARNING", "Channel 1 was closed: ChannelClosedByClient: (200) 'Normal shutdown'"),
        ("WARNING", "Connection closed, reconnecting in 1s: ConnectionClosedByClient: (200) 'Normal shutdown'"),
        ("WARNING", "Channel 1 was closed: ChannelClosedByClient: (200) 'Normal shutdown'"),
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
        ("WARNING", "Channel 1 was closed: ChannelClosedByClient: (200) 'Normal shutdown'"),
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
        ("WARNING", "Channel 1 was closed: ChannelClosedByClient: (200) 'Normal shutdown'"),
    ]
