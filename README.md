# Overview
Python wrapper to DataWarehouse Connect.
-   Free software: ISC license

## Installation
`conda install -c conda-forge dwclib`

`pip install dwclib`

Installation through conda greatly simplifies dependency management.
You can also install the in-development version with:

`pip install https://framagit.org/jaj/dwclib/-/archive/master/dwclib-master.zip`

## Changelog
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

