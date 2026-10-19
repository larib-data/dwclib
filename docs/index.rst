The dwclib Project
==============================

.. toctree::
   :hidden:
   :maxdepth: 1

   patients
   numerics
   waves
   alerts
   enumerations
   license


A Python library which enables queries against Philips DataWarehouse Connect
databases in a simple and fast manner. Patients, numerics, waveforms, alerts and
enumerations are returned as pandas dataframes. Optional dask support is
available through the ``dask`` extra; ``import dwclib`` never imports dask.


Installation
------------

dwclib needs Python 3.13 or newer:

.. code-block:: console

   $ pip install dwclib            # DWC only
   $ pip install 'dwclib[dwcmeta]' # also search patients in DWCmeta
   $ uv add 'dwclib[dwcmeta]'      # in a uv project


Configuration
-------------

Connection URIs are read from ``larib-data/config.ini`` in the user config
directory (``~/.config/larib-data/config.ini`` on Linux), sections ``[dwc]``
and ``[dwcmeta]``. Leave the ``uri`` argument out to use them. The file is read
at call time, so :func:`dwclib.common.db.update_config` takes effect at once.

Time windows are half-open: ``dtbegin <= TimeStamp < dtend``.


MCP server
----------

The MCP server that used to ship with dwclib (the ``mcp`` extra and the
``dwclib-mcp`` command) has moved to
`laribdb_mcp <https://github.com/larib-data/laribdb_mcp>`_, which serves DWC
monitoring together with ICCA anesthesia charting, with pseudonymized outputs.
Its ``dwc_*`` tools replace the former ``dwclib_*`` tools.
