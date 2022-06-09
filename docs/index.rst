The dwclib Project
==============================

.. toctree::
   :hidden:
   :maxdepth: 1

   license
   reference


A Python library which enables queries against Philips DataWarehouse Connect
databases in a simple and fast manner. Numerics and Waveform data can be
queried and is returned as pandas dataframes. Optional dask support is included
in the library. Waveform queries are accelerated with the numba JIT compiler.


Installation
------------

To install dwclib, run this command in your terminal:

.. code-block:: console

   $ conda install -c conda-forge dwclib
