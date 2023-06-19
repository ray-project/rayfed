Usage
=====

.. _installation:

Installation
------------

To use RayFed, first install it using pip:

.. code-block:: console

   (.venv) $ pip install -U rayfed

Starting RayFed
---------------

To start a RayFed application, you can use ``fed.init()`` function:

.. autofunction:: fed.init

For example:

>>> import ray
>>> import fed
>>> ray.init()
>>> fed.init(cluster=cluster, party="Alice", tls_config=tls_config)

Successfully to connect to current Ray cluster in party `Alice`
