Changelog
=========

0.0.5 (2021-11-19)
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

-  Add :func:`yapw.methods.publish` to publish messages from the context of a consumer callback.

Changed
~~~~~~~

-  Pass a ``state`` object with a ``connection`` attribute to the consumer callback, instead of a ``connection`` object. Mixins can set a ``__safe__`` class attribute to list attributes that can be used safely in the consumer callback. These attributes are added to the ``state`` object.
-  Log debug messages when publishing, consuming and acknowledging messages.

0.0.1 (2021-11-19)
------------------

First release.
