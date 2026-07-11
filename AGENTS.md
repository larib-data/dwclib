# AGENTS.md

Guidance for AI coding agents working with **dwclib** — a Python wrapper over
Philips DataWarehouse Connect (DWC). It queries patient monitoring data
(numerics, high-frequency waveforms, enumerations, alerts) and returns pandas
DataFrames.

## Use the dwclib MCP server for DWC data

**dwclib ships an MCP server for patient discovery. Use it.** When a task
involves finding DWC patients or discovering what data exists for them — patient
IDs, data time bounds, available wave/numeric labels, bed/clinical unit — call
the dwclib MCP tools instead of writing ad-hoc SQL or guessing the schema.

All tools are read-only. Primary and secondary surfaces:

- `dwclib_search_patients` — **primary** discovery tool (DWCmeta / Postgres).
  Returns patient IDs, time bounds, available wave/numeric labels, and bed/unit.
- `dwclib_search_patients_native` — MSSQL fallback for when DWCmeta is
  unavailable (no label arrays, but still IDs, time bounds, bed/unit).

The summary tools below take just a `patient_id` and summarise that patient's
whole stay — no separate bounds-discovery step. Pass optional `dtbegin`/`dtend`
to narrow the window; each defaults to the patient's full data range.

- `dwclib_numerics_summary` — per-signal stats (count, min, max, mean, std) and
  time coverage.
- `dwclib_enumerations_summary` — categorical stats (distinct/value counts).
- `dwclib_read_alerts` — monitor alerts, aggregated and returned directly.

## Workflow: discover, then generate code

The tools return only metadata and summaries — a single waveform or numerics
query can return millions of high-frequency samples, far too large to pass
through the MCP context. So **pull bulk data with your own code**, not through
MCP:

1. **Discover** with `dwclib_search_patients` (or `_native`) to find a patient
   and the labels / time window available for them.
2. **Read the `dwclib://reference` MCP resource** to get the exact signatures
   and return shapes of the public `read_*` functions (generated live from the
   installed library, so it never drifts).
3. **Generate and run Python** that calls `dwclib` directly to pull the bulk
   data, e.g. `read_numerics(patientid, dtbegin, dtend, labels)` or
   `read_waves(patientid, dtbegin, dtend, labels)`.

Notes for generated code:

- Time bounds are **half-open**: `dtbegin <= TimeStamp < dtend`. `dtbegin` /
  `dtend` accept ISO-8601 strings or datetimes.
- Connection URIs come from the **server-side config file**
  (`larib-data/config.ini`), so **omit the `uri` argument** — credentials are
  never passed through tool or function arguments.
- Keep waveform/numerics time windows narrow.

## Making the server available

The MCP server is an optional extra. Install it and register the `dwclib-mcp`
stdio command in your MCP client:

```console
$ pip install 'dwclib[mcp]'      # or: uv sync --extra mcp
```

Client-neutral `mcpServers` config (works across Claude Code, Cursor, and
similar clients):

```json
{ "mcpServers": { "dwclib": { "command": "dwclib-mcp" } } }
```

Full reference: `docs/mcp.rst`.

## Working in this repo

- Source lives under `src/dwclib/`; the MCP server is `src/dwclib/mcp/`.
- Lint, audit, and docs run through **nox** (`noxfile.py`): `nox -s lint`,
  `nox -s audit`, `nox -s docs`. Packaging and dependencies are managed with
  **uv** (`pyproject.toml`, `uv.lock`).
