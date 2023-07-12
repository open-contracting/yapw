#!/usr/bin/env python
"""
This script can be used to test error handling.
"""

import logging
import sys

from yapw.clients import AsyncConsumer, Blocking
from yapw.decorators import requeue

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)


def callback(state, channel, method, properties, body):
    raise Exception("message")


def main():
    kwargs = {"durable": False, "exchange": "yapw_development"}
    consumer_kwargs = {"on_message_callback": callback, "queue": "raise", "decorator": requeue}
    if len(sys.argv) > 1 and sys.argv[1] == "Blocking":
        print("Blocking consumer")
        client = Blocking(**kwargs)
        client.consume(**consumer_kwargs)
    else:
        print("Asynchronous consumer")
        client = AsyncConsumer(**kwargs, **consumer_kwargs)
        client.start()


if __name__ == "__main__":
    main()
