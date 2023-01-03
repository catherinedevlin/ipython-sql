Python API
==========

JupySQL is primarily used via the ``%sql``, ``%%sql``, and ``%sqlplot`` magics; however
there is a public Python API you can also use.

``sql.plot``
------------

.. note::

    ``sql.plot`` requires ``matplotlib``: ``pip install matplotlib``


The ``sql.plot`` module implements functions that compute the summary statistics
in the database, a much more scalable approach that loading all your data into
memory with pandas.

``histogram``
*************

.. autofunction:: sql.plot.histogram


``boxplot``
***********

.. autofunction:: sql.plot.boxplot


``sql.store``
-------------

The ``sql.store`` module implements utilities to compose and manage large SQL queries


``SQLStore``
************

.. autoclass:: sql.store.SQLStore
    :members:

