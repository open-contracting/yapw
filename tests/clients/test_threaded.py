import functools
import json
import logging
import os
import signal
import time

import pytest

from yapw.clients import Base, Blocking, Threaded, Transient
from yapw.decorators import discard, requeue
from yapw.methods.blocking import ack, nack, publish

logger = logging.getLogger(__name__)

DELAY = 0.05
RABBIT_URL = os.getenv("TEST_RABBIT_URL", "amqp://127.0.0.1")


class Client(Threaded, Transient, Blocking, Base):
    pass


def get_client(**kwargs):
    return Client(url=RABBIT_URL, exchange="yapw_test", **kwargs)


def encode(message):
    if not isinstance(message, bytes):
        return json.dumps(message, separators=(",", ":")).encode()
    return message


def kill(signum):
    os.kill(os.getpid(), signum)
    # The signal should be handled once.
    os.kill(os.getpid(), signum)


@pytest.fixture(params=[({}, {"message": "value"}), ({"content_type": "application/octet-stream"}, b"message value")])
def message(request):
    kwargs, body = request.param

    publisher = get_client(**kwargs)
    publisher.declare_queue("q")
    publisher.publish(body, "q")
    yield body
    # Purge the queue, instead of waiting for a restart.
    publisher.channel.queue_purge("yapw_test_q")
    publisher.close()


@pytest.fixture
def short_message(request):
    body = 1

    publisher = get_client()
    publisher.declare_queue("q")
    publisher.publish(body, "q")
    yield body
    # Purge the queue, instead of waiting for a restart.
    publisher.channel.queue_purge("yapw_test_q")
    publisher.close()


# Consumer callbacks.
def sleeper(state, channel, method, properties, body):
    logger.info("Sleep")
    time.sleep(DELAY * 2)
    logger.info("Wake!")
    ack(state, channel, method.delivery_tag)


def raiser(state, channel, method, properties, body):
    raise Exception("message")


def ack_warner(state, channel, method, properties, body):
    logger.warning(body)
    ack(state, channel, method.delivery_tag)


def nack_warner(state, channel, method, properties, body):
    logger.warning(body)
    nack(state, channel, method.delivery_tag)


def writer(state, channel, method, properties, body):
    publish(state, channel, {"message": "value"}, "n")
    ack(state, channel, method.delivery_tag)


# Decoders
def decode(index, body, content_type):
    return body.decode()[index]


@pytest.mark.parametrize(
    "signum,signame",
    [(signal.SIGINT, "SIGINT"), (signal.SIGTERM, "SIGTERM"), (signal.SIGUSR1, "SIGUSR1"), (signal.SIGUSR2, "SIGUSR2")],
)
def test_shutdown(signum, signame, message, caplog):
    caplog.set_level(logging.INFO)

    consumer = get_client()
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
    consumer = get_client(decode=functools.partial(decode, 0))
    consumer.connection.call_later(DELAY, functools.partial(kill, signal.SIGINT))
    consumer.consume(ack_warner, "q")

    assert consumer.channel.is_closed
    assert consumer.connection.is_closed

    assert len(caplog.records) == 1
    assert caplog.records[-1].levelname == "WARNING"
    assert caplog.records[-1].message == "1"


def test_decode_invalid(short_message, caplog):
    caplog.set_level(logging.INFO)

    consumer = get_client(decode=functools.partial(decode, 10))
    consumer.connection.call_later(DELAY, functools.partial(kill, signal.SIGINT))
    consumer.consume(ack_warner, "q")

    assert consumer.channel.is_closed
    assert consumer.connection.is_closed

    assert len(caplog.records) == 2
    assert [(r.levelname, r.message, r.exc_info is None) for r in caplog.records] == [
        ("ERROR", f"{encode(short_message)} can't be decoded, sending SIGUSR2", False),
        ("INFO", "Received SIGUSR2, shutting down gracefully", True),
    ]


def test_decode_raiser(message, caplog):
    caplog.set_level(logging.INFO)

    consumer = get_client(decode=raiser)
    consumer.connection.call_later(DELAY, functools.partial(kill, signal.SIGINT))
    consumer.consume(ack_warner, "q")

    assert consumer.channel.is_closed
    assert consumer.connection.is_closed

    assert len(caplog.records) == 2
    assert [(r.levelname, r.message, r.exc_info is None) for r in caplog.records] == [
        ("ERROR", f"{encode(message)} can't be decoded, sending SIGUSR2", False),
        ("INFO", "Received SIGUSR2, shutting down gracefully", True),
    ]


def test_halt(message, caplog):
    caplog.set_level(logging.INFO)

    consumer = get_client()
    consumer.connection.call_later(30, functools.partial(kill, signal.SIGINT))  # in case not halted
    consumer.consume(raiser, "q")

    assert consumer.channel.is_closed
    assert consumer.connection.is_closed

    assert len(caplog.records) == 2
    assert [(r.levelname, r.message, r.exc_info is None) for r in caplog.records] == [
        ("ERROR", f"Unhandled exception when consuming {encode(message)}, sending SIGUSR1", False),
        ("INFO", "Received SIGUSR1, shutting down gracefully", True),
    ]


def test_discard(message, caplog):
    caplog.set_level(logging.INFO)

    consumer = get_client()
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

    consumer = get_client()
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

    consumer = get_client()
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
    declarer = get_client()
    declarer.connection.call_later(DELAY, functools.partial(kill, signal.SIGINT))
    declarer.consume(raiser, "q")

    publisher = get_client()
    publisher.publish({"message": "value"}, "q")

    consumer = get_client()
    consumer.connection.call_later(DELAY, functools.partial(kill, signal.SIGINT))
    consumer.consume(nack_warner, "q")

    publisher.channel.queue_purge("yapw_test_q")
    publisher.close()

    assert consumer.channel.is_closed
    assert consumer.connection.is_closed

    assert len(caplog.records) > 1
    assert all(r.levelname == "WARNING" and r.message == "{'message': 'value'}" for r in caplog.records)


def test_consume_declares_queue_routing_keys(caplog):
    declarer = get_client()
    declarer.connection.call_later(DELAY, functools.partial(kill, signal.SIGINT))
    declarer.consume(raiser, "q", ["r", "k"])

    publisher = get_client()
    publisher.publish({"message": "r"}, "r")
    publisher.publish({"message": "k"}, "k")

    consumer = get_client()
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
