MCP Server
==========

.. contents::
    :local:
    :backlinks: none

dwclib ships an optional `Model Context Protocol <https://modelcontextprotocol.io>`_
(MCP) server that exposes patient-discovery tools to LLM agents. The server is built
with `fastmcp <https://gofastmcp.com>`_ and speaks the stdio transport.

The server's main job is **patient discovery**: it helps an agent find patients and
learn what data exists for them (patient IDs, available time ranges, available
wave/numeric labels, bed/clinical unit). From there the agent is expected to **write
Python code that calls dwclib directly** to fetch the bulk data. Waveform and numerics
queries can return millions of high-frequency samples per query — far too large to pass
through an MCP context — so the tools deliberately return only summaries and metadata,
and raw retrieval is left to agent-generated code.


Installation
------------

fastmcp is an optional extra:

.. code-block:: console

   $ pip install 'dwclib[mcp]'

or, with uv:

.. code-block:: console

   $ uv sync --extra mcp


Launching
---------

Installing the extra provides a console-script entry point that runs the stdio server:

.. code-block:: console

   $ dwclib-mcp

Configure your MCP client to launch this command. The server reads its database
connection settings (``dwcuri``/``pguri``) from the library's config file
(``larib-data/config.ini``), so credentials stay server-side and are never passed
through tool arguments.


Tools
-----

**Patient discovery (primary):**

``dwclib_search_patients``
   Wraps :func:`dwclib.read_patients` (Postgres DWCmeta). Returns patient IDs, data
   time bounds, available wave/numeric labels, and bed/unit — the inputs an agent needs
   to generate correct dwclib calls. Has its own ``limit``.

``dwclib_search_patients_native``
   Wraps :func:`dwclib.read_patients_dwc_native` (MSSQL). Fallback for when the DWCmeta
   database is unavailable: no label arrays, but still IDs, time bounds, and bed/unit.

**Convenience summaries (secondary):**

``dwclib_numerics_summary``
   Wraps :func:`dwclib.read_numerics`. Per-signal statistics (count, min, max, mean,
   std) and time coverage.

``dwclib_waves_summary``
   Wraps :func:`dwclib.read_waves`. Per-label statistics from the unfolded frame.

``dwclib_enumerations_summary``
   Wraps :func:`dwclib.read_enumerations`. Categorical statistics (distinct count, value
   counts) and time coverage.

``dwclib_read_alerts``
   Wraps :func:`dwclib.read_alerts`. Already aggregated and small enough to return
   directly.

Convenience tools accept a ``response_format`` argument (``json`` default, or
``markdown``). All tools are read-only and never modify the databases.


The code-generation reference resource
--------------------------------------

The server exposes the public API reference as an MCP resource at
``dwclib://reference``. It is generated dynamically from the live
:func:`inspect.signature` and docstrings of the nine public ``read_*`` functions, so it
always matches the installed library, plus a short curated header with an end-to-end
example. Agents read this resource on demand to learn the exact signatures and return
shapes before generating dwclib code.


Workflow
--------

The intended agent workflow is **discover, then generate code**:

1. Call ``dwclib_search_patients`` to find a patient and discover the available labels
   and data time bounds.
2. Read ``dwclib://reference`` to get the exact signatures of the ``read_*`` functions.
3. Generate and run Python that calls dwclib directly — e.g.
   ``read_numerics(patientid, dtbegin, dtend, labels)`` or
   ``read_waves(patientid, dtbegin, dtend, labels)`` — using the discovered labels and
   time window to pull the bulk data outside the MCP context.
