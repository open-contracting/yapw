Changelog
=========

0.0.13 (2022-01-28)
-------------------

Fixed
~~~~~

-  Make thread management thread-safe in :class:`yapw.clients.Threaded`.

0.0.12 (2022-01-27)
-------------------

Fixed
~~~~~

-  Eliminate a memory leak in :class:`yapw.clients.Threaded`.

0.0.11 (2022-01-27)
-------------------

Added
~~~~~

-  :meth:`yapw.clients.Publisher.declare_queue` and :meth:`yapw.clients.Threaded.consume` accept an ``arguments`` keyword argument.

0.0.10 (2022-01-24)
-------------------

Fixed
~~~~~

-  :meth:`yapw.clients.Threaded.consume` cleans up threads and closes the connection (regression in 0.0.9).

0.0.9 (2022-01-24)
------------------

Fixed
~~~~~

-  :meth:`yapw.clients.Threaded.consume` no longer attempts to close a closed connection.

0.0.8 (2022-01-19)
------------------

Added
~~~~~

-  :meth:`yapw.decorators.decorate` passes the exception instance to the ``errback`` function via its ``exception`` argument.

0.0.7 (2022-01-18)
------------------

Added
~~~~~

-  :meth:`yapw.decorators.decorate` accepts a ``finalback`` keyword argument.

0.0.6 (2022-01-17)
------------------

Added
~~~~~

-  :meth:`yapw.clients.Publisher.declare_queue` and :meth:`yapw.clients.Consumer.consume`: Rename the ``routing_key`` argument to ``queue``, and add a ``routing_keys`` optional argument.

Changed
~~~~~~~

-  Log a debug message when consuming each message.

0.0.5 (2021-11-22)
------------------

Added
~~~~~

-  :class:`yapw.clients.Threaded` accepts a ``decode`` keyword argument.
-  All :mod:`yapw.decorators` functions pass decoded messages to consumer callbacks.

Changed
~~~~~~~

-  Add ``decode`` as first argument to :mod:`yapw.decorators` functions.
-  :class:`yapw.clients.Publisher`: Rename ``encoder`` keyword argument to ``encode``.
-  :class:`yapw.clients.Publisher`'s ``encode`` keyword argument defaults to :func:`yapw.util.default_encode`.
-  :func:`yapw.util.default_encode` encodes ``str`` to ``bytes`` and pickles non-``str`` to ``bytes``.

0.0.4 (2021-11-19)
------------------

Added
~~~~~

-  :class:`yapw.clients.Publisher` (and children) accepts ``encoder`` and ``content_type`` keyword arguments.

Changed
~~~~~~~

-  Use the ``SIGUSR1`` signal to kill the process from a thread.
-  Add the channel number to the debug message for ``publish()``.

0.0.3 (2021-11-19)
------------------

Added
~~~~~

-  Add and use :func:`yapw.decorators.halt` as the default decorator.

Changed
~~~~~~~

-  Rename :func:`yapw.decorators.rescue` to :func:`~yapw.decorators.discard`.

0.0.2 (2021-11-19)
------------------

Added
~~~~~

-  Add :func:`yapw.methods.blocking.publish` to publish messages from the context of a consumer callback.

Changed
~~~~~~~

-  Pass a ``state`` object with a ``connection`` attribute to the consumer callback, instead of a ``connection`` object. Mixins can set a ``__safe__`` class attribute to list attributes that can be used safely in the consumer callback. These attributes are added to the ``state`` object.
-  Log debug messages when publishing, consuming and acknowledging messages.

0.0.1 (2021-11-19)
------------------

First release.
