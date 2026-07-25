import signal

import pytest

from tests import blocking, timed
from yapw.clients import Async


@pytest.fixture(autouse=True)
def _drain_signals():
    """Discard any signal still pending at teardown, so it isn't delivered to the next test."""
    yield
    # SIG_IGN drops a pending signal.
    for signalnum in (signal.SIGINT, signal.SIGTERM):
        signal.signal(signalnum, signal.SIG_IGN)
    signal.signal(signal.SIGINT, signal.default_int_handler)
    signal.signal(signal.SIGTERM, signal.SIG_DFL)


# Use this in tests that terminate naturally (e.g. due to an exception), as a safety net against a hang.
@pytest.fixture
def timer(request):
    with timed(30):
        yield


@pytest.fixture(params=[({}, {"message": "value"}), ({"content_type": "application/octet-stream"}, b"message value")])
def message(request):
    kwargs, body = request.param

    publisher = blocking(**kwargs)
    publisher.declare_queue("q")
    publisher.publish(body, "q")
    yield body
    # Purge the queue, instead of waiting for a restart.
    publisher.channel.queue_purge("yapw_test_q")
    publisher.close()


@pytest.fixture
def short_message(request):
    body = 1

    publisher = blocking()
    publisher.declare_queue("q")
    publisher.publish(body, "q")
    yield body
    # Purge the queue, instead of waiting for a restart.
    publisher.channel.queue_purge("yapw_test_q")
    publisher.close()


@pytest.fixture
def short_reconnect_delay(request):
    reconnect_delay = Async.RECONNECT_DELAY
    Async.RECONNECT_DELAY = 1
    try:
        yield
    finally:
        Async.RECONNECT_DELAY = reconnect_delay
