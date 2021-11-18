#!/usr/bin/env python
import logging
import time

from yapw import clients
from yapw.methods import ack

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)


class Client(clients.Threaded, clients.Durable, clients.Blocking, clients.Base):
    pass


def callback(connection, channel, method, properties, body):
    logger.info("Asleep...")
    time.sleep(10)
    logger.info("Awake!")
    ack(connection, channel, method.delivery_tag)


def main():
    client = Client(exchange="ocdskingfisherexport_test")
    client.consume(callback, "sleep")


if __name__ == "__main__":
    main()
