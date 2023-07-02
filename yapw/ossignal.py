# Copied and adapted from https://github.com/scrapy/scrapy/blob/master/scrapy/utils/ossignal.py
import signal
import threading
from collections.abc import Callable
from types import FrameType

signal_names = {}
for signame in dir(signal):
    if signame.startswith("SIG") and not signame.startswith("SIG_"):
        signum = getattr(signal, signame)
        if isinstance(signum, int):
            signal_names[signum] = signame


def install_signal_handlers(function: Callable[[int, FrameType | None], None] | signal.Handlers) -> None:
    """
    Installs handlers for the SIGTERM and SIGINT signals.

    :param function: the handler
    """
    # Only the main thread is allowed to set a signal handler.
    # https://docs.python.org/3/library/signal.html
    if threading.current_thread() == threading.main_thread():
        signal.signal(signal.SIGTERM, function)
        signal.signal(signal.SIGINT, function)
