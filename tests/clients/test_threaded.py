import functools
import logging
import os
import signal
import time

import pytest

from yapw.clients import Base, Blocking, Threaded, Transient
from yapw.decorators import requeue
from yapw.methods import ack, nack, publish

logger = logging.getLogger(__name__)

DELAY = 0.05
RABBIT_URL = os.getenv("TEST_RABBIT_URL", "amqp://127.0.0.1")


class Client(Threaded, Transient, Blocking, Base):
    pass


def get_client():
    return Client(url=RABBIT_URL, exchange="yapw_test")


@pytest.fixture
def message():
    publisher = get_client()
    publisher.declare_queue("q")
    publisher.publish({}, "q")
    yield
    # Purge the queue, instead of waiting for a restart.
    publisher.channel.queue_purge("yapw_test_q")
    publisher.close()


def sleeper(state, channel, method, properties, body):
    logger.info("Sleep")
    time.sleep(DELAY * 2)
    logger.info("Wake!")
    ack(state, channel, method.delivery_tag)


def raiser(state, channel, method, properties, body):
    raise Exception("message")


def warner(state, channel, method, properties, body):
    logger.warning("Oh!")
    nack(state, channel, method.delivery_tag)


def writer(state, channel, method, properties, body):
    publish(state, channel, {"message": "value"}, "r")
    ack(state, channel, method.delivery_tag)


def kill(signum):
    os.kill(os.getpid(), signum)
    # The signal should be handled once.
    os.kill(os.getpid(), signum)


@pytest.mark.parametrize("signum,signame", [(signal.SIGINT, "SIGINT"), (signal.SIGTERM, "SIGTERM")])
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


def test_halt(message, caplog):
    caplog.set_level(logging.INFO)

    consumer = get_client()
    consumer.connection.call_later(30, functools.partial(kill, signal.SIGINT))  # in case not halted
    consumer.consume(raiser, "q")

    assert consumer.channel.is_closed
    assert consumer.connection.is_closed

    assert len(caplog.records) == 2
    # raise Exception(repr([r for r in caplog.records]))
    assert [(r.levelname, r.message, r.exc_info is None) for r in caplog.records] == [
        ("ERROR", "Unhandled exception when consuming b'{}', sending SIGTERM", False),
        ("INFO", "Received SIGTERM, shutting down gracefully", True),
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
        ("ERROR", "Unhandled exception when consuming b'{}' (requeue=True)", False),
        ("ERROR", "Unhandled exception when consuming b'{}' (requeue=False)", False),
        ("INFO", "Received SIGINT, shutting down gracefully", True),
    ]


def test_publish(message, caplog):
    caplog.set_level(logging.DEBUG)

    consumer = get_client()
    consumer.connection.call_later(DELAY, functools.partial(kill, signal.SIGINT))
    consumer.consume(writer, "q")

    assert consumer.channel.is_closed
    assert consumer.connection.is_closed

    assert len(caplog.records) == 4
    assert [(r.levelname, r.message) for r in caplog.records] == [
        ("DEBUG", "Consuming messages on channel 1 from queue yapw_test_q"),
        ("DEBUG", "Published message {'message': 'value'} to exchange yapw_test with routing key yapw_test_r"),
        ("DEBUG", "Ack'd message on channel 1 with delivery tag 1"),
        ("INFO", "Received SIGINT, shutting down gracefully"),
    ]


def test_consume_declares_queue(caplog):
    declarer = get_client()
    declarer.connection.call_later(DELAY, functools.partial(kill, signal.SIGINT))
    declarer.consume(warner, "q")

    publisher = get_client()
    publisher.publish({}, "q")

    consumer = get_client()
    consumer.connection.call_later(DELAY, functools.partial(kill, signal.SIGINT))
    consumer.consume(warner, "q")

    publisher.channel.queue_purge("yapw_test_q")
    publisher.close()

    assert consumer.channel.is_closed
    assert consumer.connection.is_closed

    assert len(caplog.records) > 1
    assert all(r.levelname == "WARNING" and r.message == "Oh!" for r in caplog.records)
