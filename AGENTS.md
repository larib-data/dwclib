# AGENTS.md

Guidance for AI coding agents working with **dwclib** — a Python wrapper over
Philips DataWarehouse Connect (DWC). It queries patient monitoring data
(numerics, high-frequency waveforms, enumerations, alerts) and returns pandas
DataFrames.

## MCP server: use temple_mcp

The MCP server that used to ship here (`dwclib-mcp`, the `mcp` extra) has moved to
**temple_mcp** (`github.com/larib-data/temple_mcp`), which serves DWC monitoring
data together with ICCA anesthesia charting data. Use its `dwc_*` tools for
patient discovery and summaries instead of writing ad-hoc SQL.

## Notes for code that calls dwclib

- Time bounds are **half-open**: `dtbegin <= TimeStamp < dtend`. `dtbegin` /
  `dtend` accept ISO-8601 strings or datetimes.
- Connection URIs come from the **config file** (`larib-data/config.ini`), so
  **omit the `uri` argument**. The config is read at call time, so
  `dwclib.common.db.update_config()` takes effect immediately.
- `read_patients` filters by `ipp=` (column `lifetimeid`) and `nda=`
  (column `encounterid`).
- Keep waveform/numerics time windows narrow: one query can return millions of
  samples.
- `dask` is an optional extra; `import dwclib` never imports it.

## Working in this repo

- Source lives under `src/dwclib/`, tests under `tests/`.
- Packaging and dependencies are managed with **uv** (`uv_build` backend).
- Lint and format: `uv run ruff check` and `uv run ruff format`.
- Type-check: `uv run ty check`. Test: `uv run pytest`.
- Docs: `uv run --group docs sphinx-build docs docs/_build`.
- Audit: `uvx pip-audit`.
