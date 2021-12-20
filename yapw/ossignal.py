# Copied and adapted from https://github.com/scrapy/scrapy/blob/master/scrapy/utils/ossignal.py
import signal
from types import FrameType
from typing import Callable, Optional, Union

signal_names = {}
for signame in dir(signal):
    if signame.startswith("SIG") and not signame.startswith("SIG_"):
        signum = getattr(signal, signame)
        if isinstance(signum, int):
            signal_names[signum] = signame


def install_signal_handlers(function: Union[Callable[[int, Optional[FrameType]], None], signal.Handlers]) -> None:
    """
    Installs handlers for the SIGTERM, SIGINT, SIGUSR1 and SIGUSR2 signals.

    :param function: the handler
    """
    signal.signal(signal.SIGTERM, function)
    signal.signal(signal.SIGINT, function)
    signal.signal(signal.SIGUSR1, function)
    signal.signal(signal.SIGUSR2, function)
