import functools
import logging
import os
import signal
import time

import pytest

from yapw.clients import Base, Blocking, Threaded, Transient
from yapw.decorators import requeue
from yapw.methods import ack

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


def sleeper(connection, channel, method, properties, body):
    logger.info("Sleep")
    time.sleep(DELAY * 2)
    logger.info("Wake!")
    ack(connection, channel, method.delivery_tag)


def raiser(connection, channel, method, properties, body):
    raise Exception("message")


def warner(connection, channel, method, properties, body):
    logger.warning("Oh!")


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


def test_decorator(message, caplog):
    caplog.set_level(logging.INFO)

    consumer = get_client()
    consumer.connection.call_later(DELAY, functools.partial(kill, signal.SIGTERM))
    consumer.consume(raiser, "q", decorator=requeue)

    assert consumer.channel.is_closed
    assert consumer.connection.is_closed

    assert len(caplog.records) == 3
    assert [(r.levelname, r.message, r.exc_info is None) for r in caplog.records] == [
        ("ERROR", "nack requeue=True body=b'{}'", False),
        ("ERROR", "nack requeue=False body=b'{}'", False),
        ("INFO", "Received SIGTERM, shutting down gracefully", True),
    ]


def test_declare_queue(caplog):
    declarer = get_client()
    declarer.connection.call_later(DELAY, functools.partial(kill, signal.SIGTERM))
    declarer.consume(warner, "q", decorator=requeue)

    publisher = get_client()
    publisher.publish({}, "q")

    consumer = get_client()
    consumer.connection.call_later(DELAY, functools.partial(kill, signal.SIGTERM))
    consumer.consume(warner, "q", decorator=requeue)

    publisher.channel.queue_purge("yapw_test_q")
    publisher.close()

    assert consumer.channel.is_closed
    assert consumer.connection.is_closed

    assert len(caplog.records) == 1
    assert caplog.records[-1].levelname == "WARNING"
    assert caplog.records[-1].message == "Oh!"
