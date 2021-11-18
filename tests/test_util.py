from yapw.util import json_dumps


def test_json_dumps():
    assert json_dumps({"a": 1, "b": 2}) == b'{"a":1,"b":2}'
