from collections import namedtuple
from unittest.mock import create_autospec

import pytest

from yapw.methods import ack, nack

Connection = namedtuple("Connection", "is_open add_callback_threadsafe")
Channel = namedtuple("Channel", "is_open basic_ack basic_nack")


@pytest.mark.parametrize("function,suffix", [(ack, "ack"), (nack, "nack")])
@pytest.mark.parametrize("kwargs", [{}, {"multiple": True}])
def test_success(function, suffix, kwargs):
    connection = create_autospec(Connection, is_open=True)
    channel = create_autospec(Channel, is_open=True)

    function(connection, channel, 1, **kwargs)

    connection.add_callback_threadsafe.assert_called_once()

    cb = connection.add_callback_threadsafe.call_args[0][0]
    cb()

    getattr(channel, f"basic_{suffix}").assert_called_once_with(1, **kwargs)


@pytest.mark.parametrize("function,infix", [(ack, "ACK"), (nack, "NACK")])
def test_channel_closed(function, infix, caplog):
    connection = create_autospec(Connection, is_open=True)
    channel = create_autospec(Channel, is_open=False)

    function(connection, channel, 1)

    connection.add_callback_threadsafe.assert_called_once()

    cb = connection.add_callback_threadsafe.call_args[0][0]
    cb()

    getattr(channel, f"basic_{infix.lower()}").assert_not_called()

    assert len(caplog.records) == 1
    assert caplog.records[-1].levelname == "ERROR"
    assert caplog.records[-1].message == f"Can't {infix} as channel is closed or closing"


@pytest.mark.parametrize("function,infix", [(ack, "ACK"), (nack, "NACK")])
def test_connection_closed(function, infix, caplog):
    connection = create_autospec(Connection, is_open=False)
    channel = create_autospec(Channel, is_open=True)

    function(connection, channel, 1)

    connection.add_callback_threadsafe.assert_not_called()

    assert len(caplog.records) == 1
    assert caplog.records[-1].levelname == "ERROR"
    assert caplog.records[-1].message == f"Can't {infix} as connection is closed or closing"
