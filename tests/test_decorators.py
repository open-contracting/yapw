from collections import namedtuple
from unittest.mock import patch

import pytest

from yapw.decorators import discard, requeue

# https://pika.readthedocs.io/en/stable/modules/spec.html#pika.spec.Basic.Deliver
Deliver = namedtuple("Deliver", "delivery_tag redelivered")


def raises(*args):
    raise Exception("message")


def closes(*args):
    global opened
    opened = True
    try:
        raise Exception("message")
    finally:
        opened = False


@patch("yapw.decorators.nack")
def test_discard(nack, caplog):
    method = Deliver(1, False)

    discard(raises, "connection", "channel", method, "properties", b"body")

    nack.assert_called_once_with("connection", "channel", 1, requeue=False)

    assert len(caplog.records) == 1
    assert caplog.records[-1].levelname == "ERROR"
    assert caplog.records[-1].message == "Unhandled exception when consuming b'body' (requeue=False)"
    assert caplog.records[-1].exc_info


@pytest.mark.parametrize("redelivered,requeue_kwarg", [(False, True), (True, False)])
@patch("yapw.decorators.nack")
def test_requeue(nack, redelivered, requeue_kwarg, caplog):
    method = Deliver(1, redelivered)

    requeue(raises, "connection", "channel", method, "properties", b"body")

    nack.assert_called_once_with("connection", "channel", 1, requeue=requeue_kwarg)

    assert len(caplog.records) == 1
    assert caplog.records[-1].levelname == "ERROR"
    assert caplog.records[-1].message == f"Unhandled exception when consuming b'body' (requeue={requeue_kwarg})"
    assert caplog.records[-1].exc_info


@patch("yapw.decorators.nack")
def test_finally(nack):
    method = Deliver(1, False)

    discard(closes, "connection", "channel", method, "properties", b"body")

    global opened
    assert opened is False
