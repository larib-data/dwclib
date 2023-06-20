# Overview
Python wrapper to DataWarehouse Connect.
-   Free software: ISC license

## Installation
`conda install -c conda-forge dwclib`

`pip install dwclib`

Installation through conda greatly simplifies dependency management.

Additionally, Microsoft SQL Server drivers are needed and will need to be installed seperately.
See here for more information: https://github.com/mkleehammer/pyodbc/wiki


## Changelog
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

