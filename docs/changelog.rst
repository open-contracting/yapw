Changelog
=========

0.0.4 (2021-11-19)
------------------

Added
~~~~~

-  :class:`pika.clients.Publisher` (and children) accepts ``encoder`` and ``content_type`` keyword arguments.

Changed
~~~~~~~

-  Use the ``SIGUSR1`` signal to kill the process from a thread.
-  Add the channel number to the debug message for ``publish()``.

0.0.3 (2021-11-19)
------------------

Added
~~~~~

-  Add and use :meth:`pika.decorators.halt` as the default decorator.

Changed
~~~~~~~

-  Rename :meth:`pika.decorators.rescue` to :meth:`~pika.decorators.discard`.

0.0.2 (2021-11-19)
------------------

Added
~~~~~

-  Add :meth:`pika.methods.publish` to publish messages from the context of a consumer callback.

Changed
~~~~~~~

-  Pass a ``state`` object with a ``connection`` attribute to the consumer callback, instead of a ``connection`` object. Mixins can set a ``__safe__`` class attribute to list attributes that can be used safely in the consumer callback. These attributes are added to the ``state`` object.
-  Log debug messages when publishing, consuming and acknowledging messages.

0.0.1 (2021-11-19)
------------------

First release.
