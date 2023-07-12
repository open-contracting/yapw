#!/usr/bin/env python
"""
This script can be used to test signal handling.
"""

import logging
import sys
import time

import pika

from yapw.clients import AsyncConsumer, Blocking
from yapw.methods import ack

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)


def callback(state, channel, method, properties, body):
    logger.info("Sleep")
    time.sleep(5)  # Note: time.sleep() blocks the IO loop. An asynchronous version like asyncio.sleep() wouldn't.
    logger.info("Wake!")
    if isinstance(state.connection, pika.BlockingConnection):
        ack(state, channel, method.delivery_tag)
    else:
        channel.basic_ack(method.delivery_tag)


def main():
    kwargs = {"durable": False, "exchange": "yapw_development", "prefetch_count": 10}
    consumer_kwargs = {"on_message_callback": callback, "queue": "sleep"}
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
