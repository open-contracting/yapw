import functools
import json
import logging
import os
import signal
import time
from contextlib import contextmanager
from threading import Timer

from yapw.clients import Blocking
from yapw.decorators import halt
from yapw.methods import ack, add_callback_threadsafe, nack, publish

DELAY = 0.05
RABBIT_URL = os.getenv("TEST_RABBIT_URL", "amqp://127.0.0.1")

logger = logging.getLogger(__name__)


def kill(signum):
    os.kill(os.getpid(), signum)


@contextmanager
def timed(interval):
    timer = Timer(interval, functools.partial(kill, signal.SIGINT))
    timer.start()
    try:
        yield
    finally:
        timer.cancel()


def interrupt_on_consume(consumer):
    """Interrupt the client ``DELAY`` seconds after it starts consuming. Use it for a consumer with no messages."""

    def channel_consumeok_callback(method):
        consumer.connection.ioloop.call_later(DELAY, consumer.interrupt)

    consumer.channel_consumeok_callback = channel_consumeok_callback
    return consumer


def kill_after(count=1, decorator=halt, signum=signal.SIGINT):
    """Wrap a ``decorator`` to send a signal once it has processed ``count`` messages."""
    return _stop_after(count, decorator, functools.partial(kill, signum))


def interrupt_after(count=1, decorator=halt):
    """Wrap a ``decorator`` to interrupt the client once it has processed ``count`` messages."""
    return _stop_after(count, decorator, None)


def _stop_after(count, decorator, stop):
    processed = 0

    def wrapper(decode, callback, state, channel, method, properties, body):
        nonlocal processed
        decorator(decode, callback, state, channel, method, properties, body)
        processed += 1
        if processed >= count:
            # Schedule the shutdown after the decorator's ack/nack, to settle the message first.
            add_callback_threadsafe(state.connection, stop or state.interrupt)

    return wrapper


def blocking(**kwargs):
    # durable=True: RabbitMQ rejects non-durable, non-exclusive queues (transient_nonexcl_queues).
    return Blocking(durable=True, url=RABBIT_URL, exchange="yapw_test", **kwargs)


def encode(message):
    if not isinstance(message, bytes):
        return json.dumps(message, separators=(",", ":")).encode()
    return message


def decode(index, body, content_type):
    return body.decode()[index]


# Consumer callbacks.
# sleeper sends the signal itself (bind signum with functools.partial), so it arrives mid-processing.
def sleeper(signum, state, channel, method, properties, body):
    logger.info("Sleep")
    kill(signum)
    time.sleep(DELAY * 2)
    logger.info("Wake!")
    ack(state, channel, method.delivery_tag)


def raiser(state, channel, method, properties, body):
    raise RuntimeError("message")


def ack_warner(state, channel, method, properties, body):
    logger.warning(body)
    ack(state, channel, method.delivery_tag)


def nack_warner(state, channel, method, properties, body):
    logger.warning(body)
    nack(state, channel, method.delivery_tag)


def writer(state, channel, method, properties, body):
    publish(state, channel, {"message": "value"}, "n")
    ack(state, channel, method.delivery_tag)
