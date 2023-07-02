import functools
import json
import logging
import os
import signal
import time
from contextlib import contextmanager
from threading import Timer

from yapw.clients import Blocking
from yapw.methods import ack, nack, publish

DELAY = 0.05
RABBIT_URL = os.getenv("TEST_RABBIT_URL", "amqp://127.0.0.1")

logger = logging.getLogger(__name__)


def kill(signum):
    os.kill(os.getpid(), signum)
    # The signal should be handled once.
    os.kill(os.getpid(), signum)


@contextmanager
def timed(interval):
    timer = Timer(interval, functools.partial(kill, signal.SIGINT))
    timer.start()
    try:
        yield
    finally:
        timer.cancel()


def blocking(**kwargs):
    return Blocking(durable=False, url=RABBIT_URL, exchange="yapw_test", **kwargs)


def encode(message):
    if not isinstance(message, bytes):
        return json.dumps(message, separators=(",", ":")).encode()
    return message


def decode(index, body, content_type):
    return body.decode()[index]


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
