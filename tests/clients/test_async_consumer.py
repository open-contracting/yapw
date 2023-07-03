import functools
import logging
import signal
from threading import Timer
from unittest.mock import patch

import pytest

from tests import (
    DELAY,
    RABBIT_URL,
    ack_warner,
    blocking,
    decode,
    encode,
    kill,
    nack_warner,
    raiser,
    sleeper,
    timed,
    writer,
)
from yapw.clients import AsyncConsumer
from yapw.decorators import discard, halt, requeue

logger = logging.getLogger(__name__)


def async_consumer(**kwargs):
    return AsyncConsumer(durable=False, url=RABBIT_URL, exchange="yapw_test", **kwargs)


@patch("yapw.clients.AsyncioConnection")
def test_init_default(connection):
    client = AsyncConsumer(on_message_callback=raiser, queue="q")

    assert client.on_message_callback == raiser
    assert client.queue == "q"
    assert client.routing_keys == ["q"]
    assert client.decorator == halt
    assert client.arguments is None


@patch("yapw.clients.AsyncioConnection")
def test_init_kwargs(connection):
    client = AsyncConsumer(
        on_message_callback=raiser,
        queue="q",
        routing_keys=["r", "k"],
        decorator=discard,
        arguments={"x-max-priority": 1},
    )

    assert client.on_message_callback == raiser
    assert client.queue == "q"
    assert client.routing_keys == ["r", "k"]
    assert client.decorator == discard
    assert client.arguments == {"x-max-priority": 1}


@pytest.mark.parametrize(
    "signum,signame",
    [(signal.SIGINT, "SIGINT"), (signal.SIGTERM, "SIGTERM")],
)
def test_shutdown(signum, signame, message, caplog):
    caplog.set_level(logging.INFO)

    Timer(DELAY, functools.partial(kill, signum)).start()

    consumer = async_consumer(on_message_callback=sleeper, queue="q")
    consumer.start()

    assert consumer.channel.is_closed
    assert consumer.connection.is_closed

    assert len(caplog.records) == 4
    assert [(r.levelname, r.message) for r in caplog.records] == [
        ("INFO", "Sleep"),
        ("INFO", f"Received {signame}, shutting down gracefully"),
        ("INFO", "Wake!"),
        ("WARNING", "Channel 1 was closed: ChannelClosedByClient: (0) 'Normal shutdown'"),
    ]


def test_decode_valid(short_message, short_timer, caplog):
    consumer = async_consumer(on_message_callback=ack_warner, queue="q", decode=functools.partial(decode, 0))
    consumer.start()

    assert consumer.channel.is_closed
    assert consumer.connection.is_closed

    assert len(caplog.records) == 2
    assert [(r.levelname, r.message) for r in caplog.records] == [
        ("WARNING", "1"),
        ("WARNING", "Channel 1 was closed: ChannelClosedByClient: (0) 'Normal shutdown'"),
    ]


def test_decode_invalid(short_message, timer, caplog):
    caplog.set_level(logging.INFO)

    consumer = async_consumer(
        on_message_callback=ack_warner, queue="q", decode=functools.partial(decode, 10)  # IndexError
    )
    consumer.start()

    assert consumer.channel.is_closed
    assert consumer.connection.is_closed

    assert len(caplog.records) == 2
    assert [(r.levelname, r.message, r.exc_info is None) for r in caplog.records] == [
        ("ERROR", f"{encode(short_message)} can't be decoded, shutting down gracefully", False),
        ("WARNING", "Channel 1 was closed: ChannelClosedByClient: (0) 'Normal shutdown'", True),
    ]


def test_decode_raiser(message, timer, caplog):
    caplog.set_level(logging.INFO)

    consumer = async_consumer(on_message_callback=ack_warner, queue="q", decode=raiser)
    consumer.start()

    assert consumer.channel.is_closed
    assert consumer.connection.is_closed

    assert len(caplog.records) == 2
    assert [(r.levelname, r.message, r.exc_info is None) for r in caplog.records] == [
        ("ERROR", f"{encode(message)} can't be decoded, shutting down gracefully", False),
        ("WARNING", "Channel 1 was closed: ChannelClosedByClient: (0) 'Normal shutdown'", True),
    ]


def test_halt(message, timer, caplog):
    caplog.set_level(logging.INFO)

    consumer = async_consumer(on_message_callback=raiser, queue="q")
    consumer.start()

    assert consumer.channel.is_closed
    assert consumer.connection.is_closed

    assert len(caplog.records) == 2
    assert [(r.levelname, r.message, r.exc_info is None) for r in caplog.records] == [
        ("ERROR", f"Unhandled exception when consuming {encode(message)}, shutting down gracefully", False),
        ("WARNING", "Channel 1 was closed: ChannelClosedByClient: (0) 'Normal shutdown'", True),
    ]


def test_discard(message, short_timer, caplog):
    caplog.set_level(logging.INFO)

    consumer = async_consumer(on_message_callback=raiser, queue="q", decorator=discard)
    consumer.start()

    assert consumer.channel.is_closed
    assert consumer.connection.is_closed

    assert len(caplog.records) == 3
    assert [(r.levelname, r.message, r.exc_info is None) for r in caplog.records] == [
        ("ERROR", f"Unhandled exception when consuming {encode(message)}, discarding message", False),
        ("INFO", "Received SIGINT, shutting down gracefully", True),
        ("WARNING", "Channel 1 was closed: ChannelClosedByClient: (0) 'Normal shutdown'", True),
    ]


def test_requeue(message, short_timer, caplog):
    caplog.set_level(logging.INFO)

    consumer = async_consumer(on_message_callback=raiser, queue="q", decorator=requeue)
    consumer.start()

    assert consumer.channel.is_closed
    assert consumer.connection.is_closed

    assert len(caplog.records) == 4
    assert [(r.levelname, r.message, r.exc_info is None) for r in caplog.records] == [
        ("ERROR", f"Unhandled exception when consuming {encode(message)} (requeue=True)", False),
        ("ERROR", f"Unhandled exception when consuming {encode(message)} (requeue=False)", False),
        ("INFO", "Received SIGINT, shutting down gracefully", True),
        ("WARNING", "Channel 1 was closed: ChannelClosedByClient: (0) 'Normal shutdown'", True),
    ]


def test_publish(message, short_timer, caplog):
    caplog.set_level(logging.DEBUG)

    consumer = async_consumer(on_message_callback=writer, queue="q", exchange_type="direct")
    consumer.start()

    assert consumer.channel.is_closed
    assert consumer.connection.is_closed

    assert len(caplog.records) == 7
    assert [(r.levelname, r.message) for r in caplog.records] == [
        ("DEBUG", "Declaring transient direct exchange yapw_test"),
        ("DEBUG", "Consuming messages on channel 1 from queue yapw_test_q"),
        ("DEBUG", f"Received message {encode(message)} with routing key yapw_test_q and delivery tag 1"),
        (
            "DEBUG",
            "Published message {'message': 'value'} on channel 1 to exchange yapw_test with routing key yapw_test_n",
        ),
        ("DEBUG", "Ack'd message on channel 1 with delivery tag 1"),
        ("INFO", "Received SIGINT, shutting down gracefully"),
        ("WARNING", "Channel 1 was closed: ChannelClosedByClient: (0) 'Normal shutdown'"),
    ]


def test_consume_declares_queue(caplog):
    with timed(DELAY):
        declarer = async_consumer(on_message_callback=raiser, queue="q")
        declarer.start()

    publisher = blocking()
    publisher.publish({"message": "value"}, "q")

    with timed(DELAY):
        consumer = async_consumer(on_message_callback=nack_warner, queue="q")
        consumer.start()

    publisher.channel.queue_purge("yapw_test_q")
    publisher.close()

    start, end = 1, -1

    assert consumer.channel.is_closed
    assert consumer.connection.is_closed

    assert len(caplog.records[start:end]) > 1
    assert all(r.levelname == "WARNING" and r.message == "{'message': 'value'}" for r in caplog.records[start:end])

    for i, j in ((0, start), (end, len(caplog.records))):
        assert [(r.levelname, r.message) for r in caplog.records[i:j]] == [
            ("WARNING", "Channel 1 was closed: ChannelClosedByClient: (0) 'Normal shutdown'"),
        ]


def test_consume_declares_queue_routing_keys(caplog):
    with timed(DELAY):
        declarer = async_consumer(on_message_callback="raiser", queue="q", routing_keys=["r", "k"])
        declarer.start()

    publisher = blocking()
    publisher.publish({"message": "r"}, "r")
    publisher.publish({"message": "k"}, "k")

    with timed(DELAY):
        consumer = async_consumer(on_message_callback=ack_warner, queue="q", routing_keys=["r", "k"])
        consumer.start()

    publisher.channel.queue_purge("yapw_test_q")
    publisher.close()

    assert consumer.channel.is_closed
    assert consumer.connection.is_closed

    assert len(caplog.records) == 4
    assert [(r.levelname, r.message) for r in caplog.records] == [
        ("WARNING", "Channel 1 was closed: ChannelClosedByClient: (0) 'Normal shutdown'"),
        ("WARNING", "{'message': 'r'}"),
        ("WARNING", "{'message': 'k'}"),
        ("WARNING", "Channel 1 was closed: ChannelClosedByClient: (0) 'Normal shutdown'"),
    ]
