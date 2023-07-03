import functools
import logging
import signal

import pytest

from tests import DELAY, ack_warner, blocking, decode, encode, kill, nack_warner, raiser, sleeper, writer
from yapw.decorators import discard, requeue

logger = logging.getLogger(__name__)


@pytest.mark.parametrize(
    "signum,signame",
    [(signal.SIGINT, "SIGINT"), (signal.SIGTERM, "SIGTERM")],
)
def test_shutdown(signum, signame, message, caplog):
    caplog.set_level(logging.INFO)

    consumer = blocking()
    consumer.connection.call_later(DELAY, functools.partial(kill, signum))
    consumer.consume(sleeper, "q")

    assert consumer.channel.is_closed
    assert consumer.connection.is_closed

    assert len(caplog.records) == 3
    assert [(r.levelname, r.message) for r in caplog.records] == [
        ("INFO", "Sleep"),
        ("INFO", f"Received {signame}, shutting down gracefully"),
        ("INFO", "Wake!"),
    ]


def test_decode_valid(short_message, caplog):
    consumer = blocking(decode=functools.partial(decode, 0))
    consumer.connection.call_later(DELAY, functools.partial(kill, signal.SIGINT))
    consumer.consume(ack_warner, "q")

    assert consumer.channel.is_closed
    assert consumer.connection.is_closed

    assert len(caplog.records) == 1
    assert [(r.levelname, r.message) for r in caplog.records] == [("WARNING", "1")]


def test_decode_invalid(short_message, caplog):
    caplog.set_level(logging.INFO)

    consumer = blocking(decode=functools.partial(decode, 10))  # IndexError
    consumer.connection.call_later(DELAY, functools.partial(kill, signal.SIGINT))
    consumer.consume(ack_warner, "q")

    assert consumer.channel.is_closed
    assert consumer.connection.is_closed

    assert len(caplog.records) == 1
    assert [(r.levelname, r.message, r.exc_info is None) for r in caplog.records] == [
        ("ERROR", f"{encode(short_message)} can't be decoded, shutting down gracefully", False),
    ]


def test_decode_raiser(message, caplog):
    caplog.set_level(logging.INFO)

    consumer = blocking(decode=raiser)
    consumer.connection.call_later(DELAY, functools.partial(kill, signal.SIGINT))
    consumer.consume(ack_warner, "q")

    assert consumer.channel.is_closed
    assert consumer.connection.is_closed

    assert len(caplog.records) == 1
    assert [(r.levelname, r.message, r.exc_info is None) for r in caplog.records] == [
        ("ERROR", f"{encode(message)} can't be decoded, shutting down gracefully", False),
    ]


def test_halt(message, caplog):
    caplog.set_level(logging.INFO)

    consumer = blocking()
    consumer.connection.call_later(30, functools.partial(kill, signal.SIGINT))  # in case not halted
    consumer.consume(raiser, "q")

    assert consumer.channel.is_closed
    assert consumer.connection.is_closed

    assert len(caplog.records) == 1
    assert [(r.levelname, r.message, r.exc_info is None) for r in caplog.records] == [
        ("ERROR", f"Unhandled exception when consuming {encode(message)}, shutting down gracefully", False),
    ]


def test_discard(message, caplog):
    caplog.set_level(logging.INFO)

    consumer = blocking()
    consumer.connection.call_later(DELAY, functools.partial(kill, signal.SIGINT))
    consumer.consume(raiser, "q", decorator=discard)

    assert consumer.channel.is_closed
    assert consumer.connection.is_closed

    assert len(caplog.records) == 2
    assert [(r.levelname, r.message, r.exc_info is None) for r in caplog.records] == [
        ("ERROR", f"Unhandled exception when consuming {encode(message)}, discarding message", False),
        ("INFO", "Received SIGINT, shutting down gracefully", True),
    ]


def test_requeue(message, caplog):
    caplog.set_level(logging.INFO)

    consumer = blocking()
    consumer.connection.call_later(DELAY, functools.partial(kill, signal.SIGINT))
    consumer.consume(raiser, "q", decorator=requeue)

    assert consumer.channel.is_closed
    assert consumer.connection.is_closed

    assert len(caplog.records) == 3
    assert [(r.levelname, r.message, r.exc_info is None) for r in caplog.records] == [
        ("ERROR", f"Unhandled exception when consuming {encode(message)} (requeue=True)", False),
        ("ERROR", f"Unhandled exception when consuming {encode(message)} (requeue=False)", False),
        ("INFO", "Received SIGINT, shutting down gracefully", True),
    ]


def test_publish(message, caplog):
    caplog.set_level(logging.DEBUG)

    consumer = blocking()
    consumer.connection.call_later(DELAY, functools.partial(kill, signal.SIGINT))
    consumer.consume(writer, "q")

    assert consumer.channel.is_closed
    assert consumer.connection.is_closed

    assert len(caplog.records) == 5
    assert [(r.levelname, r.message) for r in caplog.records] == [
        ("DEBUG", "Consuming messages on channel 1 from queue yapw_test_q"),
        ("DEBUG", f"Received message {encode(message)} with routing key yapw_test_q and delivery tag 1"),
        (
            "DEBUG",
            "Published message {'message': 'value'} on channel 1 to exchange yapw_test with routing key yapw_test_n",
        ),
        ("DEBUG", "Ack'd message on channel 1 with delivery tag 1"),
        ("INFO", "Received SIGINT, shutting down gracefully"),
    ]


def test_consume_declares_queue(caplog):
    declarer = blocking()
    declarer.connection.call_later(DELAY, functools.partial(kill, signal.SIGINT))
    declarer.consume(raiser, "q")

    publisher = blocking()
    publisher.publish({"message": "value"}, "q")

    consumer = blocking()
    consumer.connection.call_later(DELAY, functools.partial(kill, signal.SIGINT))
    consumer.consume(nack_warner, "q")

    publisher.channel.queue_purge("yapw_test_q")
    publisher.close()

    assert consumer.channel.is_closed
    assert consumer.connection.is_closed

    assert len(caplog.records) > 1
    assert all(r.levelname == "WARNING" and r.message == "{'message': 'value'}" for r in caplog.records)


def test_consume_declares_queue_routing_keys(caplog):
    declarer = blocking()
    declarer.connection.call_later(DELAY, functools.partial(kill, signal.SIGINT))
    declarer.consume(raiser, "q", ["r", "k"])

    publisher = blocking()
    publisher.publish({"message": "r"}, "r")
    publisher.publish({"message": "k"}, "k")

    consumer = blocking()
    consumer.connection.call_later(DELAY, functools.partial(kill, signal.SIGINT))
    consumer.consume(ack_warner, "q", ["r", "k"])

    publisher.channel.queue_purge("yapw_test_q")
    publisher.close()

    assert consumer.channel.is_closed
    assert consumer.connection.is_closed

    assert len(caplog.records) == 2
    assert [(r.levelname, r.message) for r in caplog.records] == [
        ("WARNING", "{'message': 'r'}"),
        ("WARNING", "{'message': 'k'}"),
    ]
