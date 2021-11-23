import pickle

import pytest

from yapw.util import default_encode, json_dumps


def test_json_dumps():
    assert json_dumps({"a": 1, "b": 2}) == b'{"a":1,"b":2}'


@pytest.mark.parametrize(
    "args,expected",
    [
        (({"a": 1, "b": 2}, "application/json"), b'{"a":1,"b":2}'),
        (("string", None), b"string"),
        ((b"bytes", None), b"bytes"),
        (({}, None), pickle.dumps({})),
    ],
)
def test_default_encode(args, expected):
    assert default_encode(*args) == expected
