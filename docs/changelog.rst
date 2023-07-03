Changelog
=========

0.1.3 (2023-07-03)
------------------

Changed
~~~~~~~

-  :meth:`yapw.clients.Async.connection_open_error_callback`: Don't retry if the error is an authentication (e.g. incorrect username or password) or access denied (e.g. non-existent virtual host) error.
-  :meth:`yapw.clients.Async.connection_close_callback`: Change log level from ``ERROR`` to ``WARNING``.
-  :meth:`yapw.clients.Async` log messages format errors with ``%r`` instead of ``%s``, to include information like the exception class.

0.1.2 (2023-07-02)
------------------

Added
~~~~~

-  Restore Python 3.10 support.

0.1.1 (2023-07-02)
------------------

Fixed
~~~~~

-  Fix logging of exchange type.

0.1.0 (2023-07-02)
------------------

Added
~~~~~

-  :class:`yapw.clients.Async`
-  :class:`yapw.clients.AsyncConsumer`
-  :meth:`yapw.clients.Base.add_signal_handler`
-  :meth:`yapw.clients.Base.interrupt`
-  :meth:`yapw.clients.Base.state`
-  :meth:`yapw.clients.Blocking.channel_cancel_callback`
-  :meth:`yapw.clients.Blocking.add_signal_handler`
-  :meth:`yapw.clients.Blocking.interrupt`

Changed
~~~~~~~

**BREAKING CHANGES:**

-  Use subclasses instead of mixins, to share logic between synchronous and asynchronous clients with less code.
-  Move ``__init__`` arguments from other classes to the :class:`~yapw.clients.Base` class.
-  Move the ``publish`` method from the  :class:`~yapw.clients.Blocking` class to the :class:`~yapw.clients.Base` class.
-  Move and rename ``install_signal_handlers`` from ``yapw.ossignal`` to :class:`yapw.clients.Base.add_signal_handlers` class.
-  Move the ``default_decode`` method from the :mod:`yapw.decorators` module to the :mod:`yapw.util` module.
-  Rename the ``callback`` positional argument for the consumer callback to ``on_message_callback``, to avoid ambiguity.
-  Rename the ``yapw.methods.blocking`` module to the :mod:`yapw.methods` module.
-  Merge the ``Publisher`` and ``Threaded`` classes into the :class:`~yapw.clients.Blocking` class.
-  Merge the ``Durable`` and ``Transient`` classes into the :class:`~yapw.clients.Blocking` class, as a ``durable`` keyword argument.

Non-breaking changes:

-  Pending futures are cancelled during graceful shutdown.
-  Use callbacks to communicate with the main thread from other threads, instead of sending SIGUSR1 or SIGUSR2 signals.
-  The signal handlers for the :class:`~yapw.clients.Blocking` class are installed before the consumer starts, instead of during initialization.
-  Don't attempt to catch the ``pika.exceptions.ConnectionClosedByBroker`` exception in the :meth:`yapw.clients.Blocking.consume` method (can't be caught).
-  Drop Python 3.7, 3.8, 3.9, 3.10 support.

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

-  ``yapw.clients.Publisher.declare_queue`` and :meth:`yapw.clients.Threaded.consume` accept an ``arguments`` keyword argument.

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

-  ``yapw.clients.Publisher.declare_queue`` and :meth:`yapw.clients.Consumer.consume`: Rename the ``routing_key`` argument to ``queue``, and add a ``routing_keys`` optional argument.

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
-  ``yapw.clients.Publisher``: Rename ``encoder`` keyword argument to ``encode``.
-  ``yapw.clients.Publisher``'s ``encode`` keyword argument defaults to :func:`yapw.util.default_encode`.
-  :func:`yapw.util.default_encode` encodes ``str`` to ``bytes`` and pickles non-``str`` to ``bytes``.

0.0.4 (2021-11-19)
------------------

Added
~~~~~

-  ``yapw.clients.Publisher`` (and children) accepts ``encoder`` and ``content_type`` keyword arguments.

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

-  Add :func:`yapw.methods.publish` to publish messages from the context of a consumer callback.

Changed
~~~~~~~

-  Pass a ``state`` object with a ``connection`` attribute to the consumer callback, instead of a ``connection`` object. Mixins can set a ``__safe__`` class attribute to list attributes that can be used safely in the consumer callback. These attributes are added to the ``state`` object.
-  Log debug messages when publishing, consuming and acknowledging messages.

0.0.1 (2021-11-19)
------------------

First release.
