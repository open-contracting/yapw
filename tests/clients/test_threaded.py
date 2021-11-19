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


class Client(Threaded, Transient, Blocking, Base):
    pass


@pytest.fixture
def message():
    publisher = Client(exchange="ocdskingfisherexport_test")
    publisher.declare_queue("q")
    publisher.publish({}, "q")
    yield
    # Purge the queue, instead of waiting for a restart.
    publisher.channel.queue_purge("ocdskingfisherexport_test_q")
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

    consumer = Client(exchange="ocdskingfisherexport_test")
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

    consumer = Client(exchange="ocdskingfisherexport_test")
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
    declarer = Client(exchange="ocdskingfisherexport_test")
    declarer.connection.call_later(DELAY, functools.partial(kill, signal.SIGTERM))
    declarer.consume(warner, "q", decorator=requeue)

    publisher = Client(exchange="ocdskingfisherexport_test")
    publisher.publish({}, "q")

    consumer = Client(exchange="ocdskingfisherexport_test")
    consumer.connection.call_later(DELAY, functools.partial(kill, signal.SIGTERM))
    consumer.consume(warner, "q", decorator=requeue)

    publisher.channel.queue_purge("ocdskingfisherexport_test_q")
    publisher.close()

    assert consumer.channel.is_closed
    assert consumer.connection.is_closed

    assert len(caplog.records) == 1
    assert caplog.records[-1].levelname == "WARNING"
    assert caplog.records[-1].message == "Oh!"
