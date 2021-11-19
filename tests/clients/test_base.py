import pytest

from yapw.clients import Base


def test_format_routing_key_default():
    client = Base()

    formatted = client.format_routing_key("test")

    assert formatted == "test"


def test_format_routing_key_kwargs():
    client = Base(routing_key_template="{exchange}_{routing_key}")
    client.exchange = "prefix"

    formatted = client.format_routing_key("test")

    assert formatted == "prefix_test"


def test_format_routing_key_invalid():
    client = Base(routing_key_template="{invalid}_{routing_key}")

    with pytest.raises(KeyError):
        client.format_routing_key("test")
