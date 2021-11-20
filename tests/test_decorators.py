from collections import namedtuple
from unittest.mock import Mock, call, patch

import pytest

from yapw.decorators import default_decode, discard, requeue

# https://pika.readthedocs.io/en/stable/modules/spec.html#pika.spec.Basic.Deliver
Deliver = namedtuple("Deliver", "delivery_tag redelivered")
# https://pika.readthedocs.io/en/stable/modules/spec.html#pika.spec.BasicProperties
BasicProperties = namedtuple("BasicProperties", "content_type")


def raises(*args):
    raise Exception("message")


def passes(*args):
    pass


def closes(*args):
    global opened
    opened = True
    try:
        raise Exception("message")
    finally:
        opened = False


@patch("yapw.decorators.nack")
def test_decode_json(nack, caplog):
    method = Deliver(1, False)
    properties = BasicProperties("application/json")
    callback = Mock()

    discard(default_decode, callback, "state", "channel", method, properties, b'{"message": "value"}')

    callback.assert_called_once_with("state", "channel", method, properties, {"message": "value"})
    nack.assert_not_called()

    assert not caplog.records


@patch("yapw.decorators.nack")
def test_decode_bytes(nack, caplog):
    method = Deliver(1, False)
    properties = BasicProperties("application/octet-stream")
    callback = Mock()

    discard(default_decode, callback, "state", "channel", method, properties, b"message value")

    callback.assert_called_once_with("state", "channel", method, properties, b"message value")
    nack.assert_not_called()

    assert not caplog.records


@patch("yapw.decorators.nack")
def test_decode_invalid(nack, caplog):
    method = Deliver(1, False)
    properties = BasicProperties("application/json")

    discard(default_decode, passes, "state", "channel", method, properties, b"invalid")

    assert nack.call_count == 2
    nack.assert_has_calls([call("state", "channel", 1, requeue=False), call("state", "channel", 1, requeue=False)])

    assert len(caplog.records) == 2
    assert [(r.levelname, r.message, r.exc_info is None) for r in caplog.records] == [
        ("ERROR", "b'invalid' is not valid JSON, discarding message", False),
        ("ERROR", "Unhandled exception when consuming b'invalid', discarding message", False),
    ]


@patch("yapw.decorators.nack")
def test_discard(nack, caplog):
    method = Deliver(1, False)

    discard(default_decode, raises, "state", "channel", method, "properties", b'"body"')

    nack.assert_called_once_with("state", "channel", 1, requeue=False)

    assert len(caplog.records) == 1
    assert caplog.records[-1].levelname == "ERROR"
    assert caplog.records[-1].message == "Unhandled exception when consuming b'\"body\"', discarding message"
    assert caplog.records[-1].exc_info


@pytest.mark.parametrize("redelivered,requeue_kwarg", [(False, True), (True, False)])
@patch("yapw.decorators.nack")
def test_requeue(nack, redelivered, requeue_kwarg, caplog):
    method = Deliver(1, redelivered)

    requeue(default_decode, raises, "state", "channel", method, "properties", b'"body"')

    nack.assert_called_once_with("state", "channel", 1, requeue=requeue_kwarg)

    assert len(caplog.records) == 1
    assert caplog.records[-1].levelname == "ERROR"
    assert caplog.records[-1].message == f"Unhandled exception when consuming b'\"body\"' (requeue={requeue_kwarg})"
    assert caplog.records[-1].exc_info


@patch("yapw.decorators.nack")
def test_finally(nack):
    method = Deliver(1, False)
    properties = BasicProperties("application/json")

    discard(default_decode, closes, "state", "channel", method, properties, b'"body"')

    global opened
    assert opened is False
