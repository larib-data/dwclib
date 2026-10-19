# Overview
Python wrapper to DataWarehouse Connect.
-   Free software: ISC license

## Installation
`pip install dwclib` (add the `dwcmeta` extra to search patients in DWCmeta,
the `dask` extra for dask support). Python 3.13 or newer.

The MCP server that used to ship here has moved to
[laribdb_mcp](https://github.com/larib-data/laribdb_mcp).

## Development
The project is managed with [uv](https://docs.astral.sh/uv/) (`uv_build`
backend). Linting and formatting moved from nox, flake8 and darglint to ruff;
type checking uses ty.

```sh
uv sync
uv run ruff check && uv run ruff format --check
uv run ty check
uv run pytest
uv run --group docs sphinx-build -W docs docs/_build
uvx pip-audit
```

## Changelog
- Unreleased
    - pandas 3 (no dask by default; dask is an optional extra), Python 3.13+
    - `read_patients` filters and returns the NDA (`nda=`, `encounterid`)
    - Config is read at call time, so `update_config()` takes effect at once
    - Move the MCP server to laribdb_mcp
    - Replace nox, flake8 and darglint with ruff and ty; build with uv_build
    - SQLAlchemy below 2.0.42 (newer MSSQL reflection drops DWC columns)

- 2026.07.11
    - Update documentation, add MCP server, update dependencies, move from poetry to uv

- 2024.12.27
    - Support updating the config file through the API

- 2024.9.17
    - Support reading waves in binary chunks without conversion

- 2024.5.10
    - Support searching patients directly in DWC without DWCmeta

- 2024.4.4
    - Support querying enumerations
    - Update dependencies

- 2023.6.21
    - Remove all occurences of naive datetime since dask now support tz-aware
    - New config file syntax to be compatible with other libraries
    - Fix a bug when Pleth waveform return all NaN

- 2023.6.7
    - Support for querying alerts using the read_alerts call
    - Use the platformdirs package for config file location

- 2022.9.14
    - Support numeric labels and sublabels in read_patients and read_numerics
    - Support to query for multiple patients at once in read_numerics

- 2022.6.23
    - Convert packaging from flit to poetry
    - Add linting and testing with nox, flake8 and safety
    - Create scaffolding for future Sphinx documentation
    - Fix a number of bugs in corner cases (division by zero, ...)
    - Add a generic Dask wrapper to run custom DWC queries with Dask

- 2022.3.22
    - Convert packaging from old-style setup.py to flit
    - Refactor: extract common code between dask and pandas version
    - No longer relies on user defined function in the database
    - Patients: add read_patient function to fetch a single patient
    - Numerics: read_numerics patientids can be a list or a str. When it is a list, a MultiIndex is returned

