#!/usr/bin/env python
"""
This script can be used to test publishing (like the sample code in the documentation).
"""

import logging

from yapw.clients import Async

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.DEBUG)


class Publisher(Async):
    def exchange_ready(self):
        self.publish({"message": "value"}, routing_key="messages")
        self.interrupt()


def main():
    publisher = Publisher(durable=False, exchange="yapw_development")
    publisher.start()


if __name__ == "__main__":
    main()
