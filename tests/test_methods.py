import functools
from collections import namedtuple
from unittest.mock import create_autospec

import pika
import pytest

from yapw.methods import ack, nack, publish

Connection = namedtuple("Connection", "is_open add_callback_threadsafe")
Channel = namedtuple("Channel", "is_open basic_ack basic_nack basic_publish")
State = namedtuple("State", "format_routing_key connection exchange delivery_mode")

ack_nack_parameters = [(ack, "ack", [1]), (nack, "nack", [1])]
parameters = ack_nack_parameters + [(publish, "publish", [{"message": "value"}, "q"])]


def format_routing_key(exchange, routing_key):
    return f"{exchange}_{routing_key}"


def test_publish():
    connection = create_autospec(Connection, is_open=True)
    channel = create_autospec(Channel, is_open=True)
    function = functools.partial(format_routing_key, "exch")
    state = create_autospec(
        State, connection=connection, format_routing_key=function, exchange="exch", delivery_mode=2
    )

    publish(state, channel, {"message": "value"}, "q")

    connection.add_callback_threadsafe.assert_called_once()

    cb = connection.add_callback_threadsafe.call_args[0][0]
    cb()

    properties = pika.BasicProperties(delivery_mode=2, content_type="application/json")
    getattr(channel, "basic_publish").assert_called_once_with(
        exchange="exch", routing_key="exch_q", body=b'{"message":"value"}', properties=properties
    )


@pytest.mark.parametrize("function,infix,args", ack_nack_parameters)
@pytest.mark.parametrize("kwargs", [{}, {"multiple": True}])
def test_ack_nack(function, infix, args, kwargs):
    connection = create_autospec(Connection, is_open=True)
    channel = create_autospec(Channel, is_open=True)
    state = create_autospec(State, connection=connection)

    function(state, channel, *args, **kwargs)

    connection.add_callback_threadsafe.assert_called_once()

    cb = connection.add_callback_threadsafe.call_args[0][0]
    cb()

    getattr(channel, f"basic_{infix}").assert_called_once_with(*args, **kwargs)


@pytest.mark.parametrize("function,infix,args", parameters)
def test_channel_closed(function, infix, args, caplog):
    connection = create_autospec(Connection, is_open=True)
    channel = create_autospec(Channel, is_open=False)
    state = create_autospec(State, connection=connection)

    function(state, channel, *args)

    connection.add_callback_threadsafe.assert_called_once()

    cb = connection.add_callback_threadsafe.call_args[0][0]
    cb()

    getattr(channel, f"basic_{infix.lower()}").assert_not_called()

    assert len(caplog.records) == 1
    assert caplog.records[-1].levelname == "ERROR"
    assert caplog.records[-1].message == f"Can't {infix} as channel is closed or closing"


@pytest.mark.parametrize("function,infix,args", parameters)
def test_connection_closed(function, infix, args, caplog):
    connection = create_autospec(Connection, is_open=False)
    channel = create_autospec(Channel, is_open=True)
    state = create_autospec(State, connection=connection)

    function(state, channel, *args)

    connection.add_callback_threadsafe.assert_not_called()

    assert len(caplog.records) == 1
    assert caplog.records[-1].levelname == "ERROR"
    assert caplog.records[-1].message == f"Can't {infix} as connection is closed or closing"
