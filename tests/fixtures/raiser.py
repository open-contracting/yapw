#!/usr/bin/env python
"""
This script can be used to test error handling.
"""

import logging

from yapw import clients
from yapw.decorators import requeue

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)


class Client(clients.Threaded, clients.Blocking):
    pass


def callback(state, channel, method, properties, body):
    raise Exception("message")


def main():
    client = Client(durable=False, exchange="yapw_development")
    client.consume(callback, "raise", decorator=requeue)


if __name__ == "__main__":
    main()
