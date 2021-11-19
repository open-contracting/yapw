Changelog
=========

0.0.2 (Unreleased)
------------------

Added
~~~~~

-  Add :meth:`pika.methods.publish` to publish messages from the context of a consumer callback.

Changed
~~~~~~~

-  Pass a ``state`` object with a ``connection`` attribute to the consumer callback, instead of a ``connection`` object. Mixins can set a ``__safe__`` class attribute to list attributes that can be used safely in the consumer callback. These attributes are added to the ``state`` object.

-  Log a debug message on :meth:`pika.clients.Publisher.publish`.

0.0.1 (2021-11-19)
------------------

First release.
