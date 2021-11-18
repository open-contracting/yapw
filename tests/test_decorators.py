from collections import namedtuple
from unittest.mock import patch

import pytest

from yapw.decorators import requeue, rescue

# https://pika.readthedocs.io/en/stable/modules/spec.html#pika.spec.Basic.Deliver
Deliver = namedtuple("Deliver", "delivery_tag redelivered")


def raises(*args):
    raise Exception("message")


@patch("yapw.decorators.nack")
def test_rescue(nack, caplog):
    method = Deliver(1, False)

    rescue(raises, "connection", "channel", method, "properties", b"body")

    nack.assert_called_once_with("connection", "channel", delivery_tag=1, requeue=False)

    assert len(caplog.records) == 1
    assert caplog.records[-1].levelname == "ERROR"
    assert caplog.records[-1].message == "nack requeue=False body=b'body'"
    assert caplog.records[-1].exc_info


@pytest.mark.parametrize("redelivered,requeue_kwarg", [(False, True), (True, False)])
@patch("yapw.decorators.nack")
def test_requeue(nack, redelivered, requeue_kwarg, caplog):
    method = Deliver(1, redelivered)

    requeue(raises, "connection", "channel", method, "properties", b"body")

    nack.assert_called_once_with("connection", "channel", delivery_tag=1, requeue=requeue_kwarg)

    assert len(caplog.records) == 1
    assert caplog.records[-1].levelname == "ERROR"
    assert caplog.records[-1].message == f"nack requeue={requeue_kwarg} body=b'body'"
    assert caplog.records[-1].exc_info
