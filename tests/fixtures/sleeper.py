#!/usr/bin/env python
"""
This script can be used to test signal handling.
"""

import logging
import time

from yapw import clients
from yapw.methods.blocking import ack

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)


class Client(clients.Threaded, clients.Blocking):
    pass


def callback(state, channel, method, properties, body):
    logger.info("Sleep")
    time.sleep(10)
    logger.info("Wake!")
    ack(state, channel, method.delivery_tag)


def main():
    client = Client(durable=False, exchange="yapw_development")
    client.consume(callback, "sleep")


if __name__ == "__main__":
    main()
