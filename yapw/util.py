import json

try:
    import orjson

    jsonlib = orjson
except ImportError:
    jsonlib = json


def json_dumps(data):
    """
    Serializes the data to JSON formatted bytes.
    """
    if jsonlib == json:
        return json.dumps(data, separators=(",", ":")).encode()
    return orjson.dumps(data)
