import pytest

from tests import blocking, timed
from yapw.clients import Async


# Use this in tests that terminate naturally (e.g. due to an exception).
@pytest.fixture
def timer(request):
    with timed(30):
        yield


# Use this in tests that don't terminate naturally.
@pytest.fixture
def short_timer(request):
    with timed(0.05):
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
