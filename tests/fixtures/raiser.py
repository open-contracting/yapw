#!/usr/bin/env python
import logging

from yapw import clients

logger = logging.getLogger(__name__)


class Client(clients.Threaded, clients.Durable, clients.Blocking, clients.Base):
    pass


def callback(connection, channel, method, properties, body):
    raise Exception("message")


def main():
    client = Client(exchange="ocdskingfisherexport_test")
    client.consume(callback, "raise")


if __name__ == "__main__":
    main()
