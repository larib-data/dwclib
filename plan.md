# Add MCP support to dwclib (+ docstrings + docs)

## Context

`dwclib` wraps Philips DataWarehouse Connect (DWC), reading patient-monitoring data
(numerics, waveforms, alerts, enumerations, patient metadata) from a MSSQL DWC database
and a PostgreSQL DWCmeta database, returning pandas DataFrames. The public API is the
9 `read_*` callables re-exported from `src/dwclib/__init__.py`.

We want to expose this library to LLM agents through an MCP server built with **fastmcp**
(`from fastmcp import FastMCP`, the standalone v2 package the user specified).

**Intended workflow (this drives every design choice):** the MCP server's *main* job is
**patient discovery** — help the agent find patients and learn what data exists for them
(patient IDs, available time ranges, available wave/numeric labels, bed/clinical unit).
From there **the agent writes Python code that calls `dwclib` directly** to fetch the actual
data. Bulk numerics/waves data is therefore *not* pulled through MCP.

Two consequences:
1. **Documentation is central**, not incidental: the improved docstrings + Sphinx HTML docs
   are the reference the agent generates code against, and the server also exposes them as
   an MCP **resource** for on-demand reading.
2. The numerics/waves/enumerations/alerts tools are **convenience only** (quick eyeballing),
   deliberately kept lean — raw bulk retrieval is left to agent-generated code.

Waveforms (and to a lesser extent numerics) can return millions of high-frequency samples
per query — far too large for an MCP context — which is exactly why the summaries exist and
raw pulls are pushed to generated code.

## Decisions (confirmed with user)

- **Primary surface**: patient-discovery tools; they surface available labels + data time
  bounds precisely so the agent can fill in correct `dwclib` calls.
- **Code-gen reference**: exposed as an **MCP resource** (`dwclib://reference`) — signatures +
  docstrings + usage examples for the public `read_*` functions.
- **Convenience data tools (lean)**: `numerics_summary`, `waves_summary`,
  `enumerations_summary`, `read_alerts`. Raw numerics/enumerations retrieval tools are
  **dropped** (agent generates code for raw pulls).
- **Output format**: convenience tools take a `response_format` param (`json` default, `markdown`).
- **Summary stats (numeric)**: count, min, max, mean, std, and time coverage (first/last).
  Categorical (enumerations): distinct count, value counts, time coverage.
- **Transport**: **stdio only** for now.
- **Packaging**: MCP code in its own module; fastmcp is an optional extra (mirrors the
  existing `dask` extra); a `[project.scripts]` entrypoint runs the stdio server.

## Part 1 — Docstrings (Google style; also feed docs + the code-gen resource) — DONE (6032d0c)

Add Google-style docstrings (matching existing `patients.py`: `Args:` / `Returns:`; `.flake8`
sets `docstring-convention = google`, `.darglint` `strictness = short`) to the 5 undocumented
public functions, and add an **`Examples:`** section to each showing a realistic call — since
these examples are what the agent copies when generating code:

- `src/dwclib/numerics/numerics.py` → `read_numerics`
- `src/dwclib/waves/waves.py` → `read_waves`, `read_wave_chunks`
- `src/dwclib/alerts/alerts.py` → `read_alerts`
- `src/dwclib/enumerations/enumerations.py` → `read_enumerations`

Document: `patientids`/`patientid`, `dtbegin`, `dtend`, `labels` (and `sublabels`/`pivot`
where present), `uri`, the returned DataFrame shape (long vs pivoted; empty typed `*_meta`
frame when no rows; half-open `>= dtbegin`, `< dtend`; `patientids` accepting None/str/list
with single-element unwrap). The 4 patient functions already have complete docstrings —
optionally add `Examples:` to them too; otherwise leave.

Run `flake8 src/dwclib` (via `nox -s lint`) to confirm DAR checks pass.

## Part 2 — Sphinx docs coverage — DONE (8d3141e)

`docs/reference.rst` currently only auto-documents `dwclib.patients`. Extend it with
`.. automodule::` for `dwclib.numerics`, `dwclib.waves`, `dwclib.alerts`,
`dwclib.enumerations` (plus `patients`). These import cleanly (no dask/fastmcp), so
`nox -s docs` (`sphinx-build docs docs/_build`) picks up the new docstrings. Do **not**
autodoc `dwclib.dask` (guarded import raises) or `dwclib.mcp` (needs fastmcp). Add a short
hand-written `docs/mcp.rst` (install, launch, the tools + the `dwclib://reference` resource,
and the discover-then-generate-code workflow) referenced from the `index.rst` toctree.

## Part 3 — The MCP server

New package `src/dwclib/mcp/` (all MCP code contained here):

- `src/dwclib/mcp/__init__.py` — guard the `fastmcp` import like `src/dwclib/dask/__init__.py`
  guards dask: on `ModuleNotFoundError`, raise a message telling the user to
  `pip install dwclib[mcp]`.
- `src/dwclib/mcp/server.py` — `mcp = FastMCP("dwclib_mcp")`, tool + resource definitions, and
  `def main(): mcp.run()` as the stdio console-script entry point.
- `src/dwclib/mcp/formatting.py` — shared helpers (no duplication across tools):
  - `dataframe_to_response(df, response_format, max_rows)` → JSON (records + `total_rows`,
    `returned`, `truncated`, `columns`) or a markdown table with a truncation note.
  - `summarize_numeric(df)` → per-(PatientId, SubLabel) count/min/max/mean/std + first/last.
  - `summarize_categorical(df)` → per-(PatientId, Label) distinct count, value counts, first/last.
  - `summarize_waves(df)` → per-label count/min/max/mean/std + first/last from the unfolded frame.
  - `handle_error(e)` → actionable messages (missing/incomplete config → point at
    `larib-data/config.ini` and the `update_config` API; DB connection errors, etc.).

### Tools (snake_case, `dwclib_` prefix)

**Primary — patient discovery:**

| Tool | Wraps | Purpose |
|---|---|---|
| `dwclib_search_patients` | `read_patients` | Postgres DWCmeta. Return patient IDs, data time bounds, **available wave/numeric labels**, bed/unit — the inputs the agent needs to generate `dwclib` calls. Own `limit`. |
| `dwclib_search_patients_native` | `read_patients_dwc_native` | MSSQL fallback when the meta DB is unavailable (no label arrays; still IDs + time bounds + bed/unit). |

**Convenience — quick summaries (secondary):**

| Tool | Wraps | Notes |
|---|---|---|
| `dwclib_numerics_summary` | `read_numerics(pivot=False)` | per-signal stats |
| `dwclib_waves_summary` | `read_waves` | per-label stats from unfolded frame |
| `dwclib_enumerations_summary` | `read_enumerations(pivot=False)` | categorical stats |
| `dwclib_read_alerts` | `read_alerts` | already aggregated/small |

Summaries **reuse the existing public functions** with `pivot=False` and aggregate with
pandas — no new DB/query code.

### Resource — code-generation reference

- `@mcp.resource("dwclib://reference")` returning the public API reference the agent uses to
  write `dwclib` code: for each of the 9 `read_*` functions, its `inspect.signature` + its
  `__doc__`, generated dynamically (single source of truth = the docstrings from Part 1), with
  a short curated header showing an end-to-end example (search a patient → call
  `read_numerics`/`read_waves` with the discovered labels + time window).

### Design notes

- Pydantic `BaseModel` inputs: ISO-8601 string `dtbegin`/`dtend`, optional `patient_id(s)`,
  optional `labels`/`sublabels`, `response_format` enum, `max_rows` where a raw list is
  returned. All tools set `annotations`: `readOnlyHint: True`, `destructiveHint: False`,
  `openWorldHint: True`.
- Not exposed as tools: `read_patient` / `read_patient_dwc_native` (the `limit=1` case of the
  search tools) and `read_wave_chunks` / raw numerics+enumerations pulls (left to
  agent-generated code). All still documented in the `dwclib://reference` resource.
- Tools don't expose the `uri` override; they rely on the library's config-file defaults
  (`dwcuri`/`pguri` from `larib-data/config.ini`), keeping DB credentials server-side.

## Part 4 — Packaging (`pyproject.toml`)

- `[project.optional-dependencies]`: add `mcp = ["fastmcp>=2,<3"]` (mirrors the `dask` extra).
- Add the stdio-server entrypoint:
  ```toml
  [project.scripts]
  dwclib-mcp = "dwclib.mcp.server:main"
  ```
- Hatchling auto-discovers `src/dwclib/mcp`; no build-config change needed. Keep the
  already-staged `M pyproject.toml` change and add to it.

## Verification

1. **Import unaffected**: `python -c "import dwclib"` still succeeds (guarded mcp import untouched).
2. **Lint/docstrings**: `nox -s lint` (flake8 + darglint) passes over `src/dwclib`.
3. **Docs build**: `nox -s docs` produces `docs/_build/index.html` with the 5 modules'
   docstrings + the new MCP page.
4. **MCP install + import**: `uv sync --extra mcp` (or `pip install -e '.[mcp]'`), then
   `python -c "from dwclib.mcp.server import mcp"` imports cleanly and `dwclib-mcp` launches.
5. **In-memory client smoke test** (no live DB): `from fastmcp import Client; async with
   Client(mcp) as c: await c.list_tools(); await c.list_resources(); await
   c.read_resource("dwclib://reference")` — confirm the 6 tools + resource register with
   correct schemas, the reference returns readable signatures/docstrings, and calling a tool
   with no config yields the graceful "config missing — edit larib-data/config.ini" error,
   not a stack trace.
6. **If a live DWC/DWCmeta DB is reachable**: run `dwclib_search_patients` for a known
   patient/window and confirm it surfaces the label arrays + time bounds; spot-check
   `dwclib_numerics_summary`.

## Out of scope / follow-ups

- No HTTP transport (stdio only for now).
- No raw bulk-data MCP tools (that path is agent-generated `dwclib` code by design).
- No formal MCP eval suite (needs live DB data to verify answers); add later with a test DB.
