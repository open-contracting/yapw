#!/usr/bin/env python
"""
This script can be used to test signal handling.
"""

import logging
import time

from yapw import clients
from yapw.methods import ack

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)


class Client(clients.Threaded, clients.Transient, clients.Blocking, clients.Base):
    pass


def callback(connection, channel, method, properties, body):
    logger.info("Sleep")
    time.sleep(10)
    logger.info("Wake!")
    ack(connection, channel, method.delivery_tag)


def main():
    client = Client(exchange="ocdskingfisherexport_development")
    client.consume(callback, "sleep")


if __name__ == "__main__":
    main()
