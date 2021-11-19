from unittest.mock import patch

from yapw.clients import Base, Blocking


class Client(Blocking, Base):
    pass


@patch("pika.BlockingConnection")
def test_init_default(connection):
    client = Client()

    connection.assert_called_once()

    assert connection.call_args[0][0].virtual_host == "/"
    assert connection.call_args[0][0].blocked_connection_timeout == 1800

    client.channel.basic_qos.assert_called_once_with(prefetch_count=1)


@patch("pika.BlockingConnection")
def test_init_kwargs(connection):
    client = Client(
        url="https://host:1234/%2Fv?blocked_connection_timeout=10", blocked_connection_timeout=300, prefetch_count=10
    )

    connection.assert_called_once()

    assert connection.call_args[0][0].virtual_host == "/v"
    assert connection.call_args[0][0].blocked_connection_timeout == 300

    client.channel.basic_qos.assert_called_once_with(prefetch_count=10)
