"""FastMCP server exposing dwclib patient-discovery tools to LLM agents.

The server's primary job is patient discovery: it helps an agent find patients and
learn what data exists for them (IDs, time bounds, available wave/numeric labels,
bed/unit). From there the agent is expected to generate Python that calls dwclib
directly to pull the bulk data, using the ``dwclib://reference`` resource as its
code-generation reference. The convenience summary tools are deliberately lean.
"""

import inspect
from typing import List, Optional

from fastmcp import FastMCP

from dwclib import (
    read_alerts,
    read_enumerations,
    read_numerics,
    read_patient,
    read_patient_dwc_native,
    read_patients,
    read_patients_dwc_native,
    read_wave_chunks,
    read_waves,
)
from dwclib.common.dt import to_datetime
from dwclib.mcp.formatting import (
    ResponseFormat,
    config_missing,
    dataframe_to_response,
    handle_error,
    summarize_categorical,
    summarize_numeric,
    summarize_waves,
)

mcp = FastMCP("dwclib_mcp")

# Every tool only reads from the databases; advertise that to clients.
READ_ONLY = {
    "readOnlyHint": True,
    "destructiveHint": False,
    "openWorldHint": True,
}


# --------------------------------------------------------------------------- #
# Patient discovery (primary surface)
# --------------------------------------------------------------------------- #
@mcp.tool(annotations=READ_ONLY)
def dwclib_search_patients(
    patientid: Optional[str] = None,
    name: Optional[str] = None,
    firstname: Optional[str] = None,
    ipp: Optional[str] = None,
    dtbegin: Optional[str] = None,
    dtend: Optional[str] = None,
    clinicalunit: Optional[str] = None,
    bedlabel: Optional[str] = None,
    wavelabels: Optional[List[str]] = None,
    numericlabels: Optional[List[str]] = None,
    numericsublabels: Optional[List[str]] = None,
    limit: int = 20,
    response_format: ResponseFormat = ResponseFormat.json,
):
    """Search the DWCmeta database for patient stays.

    This is the primary discovery tool: it returns patient IDs, data time bounds,
    the available wave/numeric labels and bed/clinical unit — everything needed to
    generate correct dwclib calls for the bulk data. Filters are optional; combine
    them to narrow the search. Time bounds and labels are ISO-8601 / label strings.
    """  # noqa: DAR101,DAR201
    err = config_missing("pg", response_format)
    if err is not None:
        return err
    try:
        dtbegin = to_datetime(dtbegin)
        dtend = to_datetime(dtend)
        df = read_patients(
            patientid=patientid,
            name=name,
            firstname=firstname,
            ipp=ipp,
            dtbegin=dtbegin,
            dtend=dtend,
            clinicalunit=clinicalunit,
            bedlabel=bedlabel,
            wavelabels=wavelabels,
            numericlabels=numericlabels,
            numericsublabels=numericsublabels,
            limit=limit,
        )
        return dataframe_to_response(df, response_format, max_rows=limit)
    except Exception as exc:  # noqa: BLE001 - surfaced as a graceful message
        return handle_error(exc, response_format)


@mcp.tool(annotations=READ_ONLY)
def dwclib_search_patients_native(
    patientid: Optional[str] = None,
    dtbegin: Optional[str] = None,
    dtend: Optional[str] = None,
    clinicalunit: Optional[str] = None,
    bedlabel: Optional[str] = None,
    limit: int = 20,
    response_format: ResponseFormat = ResponseFormat.json,
):
    """Search the native DWC (MSSQL) database for patient stays.

    Fallback for when the DWCmeta database is unavailable. Returns patient IDs, data
    time bounds and bed/clinical unit, but no available-label arrays (those live only
    in DWCmeta).
    """  # noqa: DAR101,DAR201
    err = config_missing("dwc", response_format)
    if err is not None:
        return err
    try:
        dtbegin = to_datetime(dtbegin)
        dtend = to_datetime(dtend)
        df = read_patients_dwc_native(
            patientid=patientid,
            dtbegin=dtbegin,
            dtend=dtend,
            clinicalunit=clinicalunit,
            bedlabel=bedlabel,
            limit=limit,
        )
        return dataframe_to_response(df, response_format, max_rows=limit)
    except Exception as exc:  # noqa: BLE001
        return handle_error(exc, response_format)


# --------------------------------------------------------------------------- #
# Convenience summaries (secondary surface)
# --------------------------------------------------------------------------- #
@mcp.tool(annotations=READ_ONLY)
def dwclib_numerics_summary(
    patientids: Optional[List[str]],
    dtbegin: str,
    dtend: str,
    labels: Optional[List[str]] = None,
    sublabels: Optional[List[str]] = None,
    response_format: ResponseFormat = ResponseFormat.json,
):
    """Summarise numeric parameters over a time window.

    Returns per-signal (PatientId, Label, SubLabel) statistics — count, min, max,
    mean, std and time coverage — for a quick look. For the raw samples, generate
    dwclib code calling ``read_numerics`` (see the ``dwclib://reference`` resource).
    """  # noqa: DAR101,DAR201
    err = config_missing("dwc", response_format)
    if err is not None:
        return err
    try:
        dtbegin = to_datetime(dtbegin)
        dtend = to_datetime(dtend)
        df = read_numerics(patientids, dtbegin, dtend, labels, sublabels, pivot=False)
        return dataframe_to_response(summarize_numeric(df), response_format)
    except Exception as exc:  # noqa: BLE001
        return handle_error(exc, response_format)


@mcp.tool(annotations=READ_ONLY)
def dwclib_waves_summary(
    patientid: str,
    dtbegin: str,
    dtend: str,
    labels: Optional[List[str]] = None,
    response_format: ResponseFormat = ResponseFormat.json,
):
    """Summarise high-frequency waveforms over a time window.

    Returns per-label statistics (count, min, max, mean, std, time coverage) from the
    unfolded frame. Keep the window narrow: a raw waveform query can return millions
    of samples, which is why only summaries are exposed here — pull the samples with
    generated ``read_waves`` code.
    """  # noqa: DAR101,DAR201
    err = config_missing("dwc", response_format)
    if err is not None:
        return err
    try:
        dtbegin = to_datetime(dtbegin)
        dtend = to_datetime(dtend)
        df = read_waves(patientid, dtbegin, dtend, labels)
        return dataframe_to_response(summarize_waves(df), response_format)
    except Exception as exc:  # noqa: BLE001
        return handle_error(exc, response_format)


@mcp.tool(annotations=READ_ONLY)
def dwclib_enumerations_summary(
    patientids: Optional[List[str]],
    dtbegin: str,
    dtend: str,
    labels: Optional[List[str]] = None,
    response_format: ResponseFormat = ResponseFormat.json,
):
    """Summarise enumerations (categorical parameters) over a time window.

    Returns per-(PatientId, Label) statistics — distinct value count, value counts and
    time coverage. For the raw values, generate dwclib code calling
    ``read_enumerations``.
    """  # noqa: DAR101,DAR201
    err = config_missing("dwc", response_format)
    if err is not None:
        return err
    try:
        dtbegin = to_datetime(dtbegin)
        dtend = to_datetime(dtend)
        df = read_enumerations(patientids, dtbegin, dtend, labels, pivot=False)
        return dataframe_to_response(summarize_categorical(df), response_format)
    except Exception as exc:  # noqa: BLE001
        return handle_error(exc, response_format)


@mcp.tool(annotations=READ_ONLY)
def dwclib_read_alerts(
    patientids: Optional[List[str]],
    dtbegin: str,
    dtend: str,
    response_format: ResponseFormat = ResponseFormat.json,
    max_rows: int = 200,
):
    """Read patient-monitor alerts over a time window.

    Alerts are already aggregated to one row per alarm and small enough to return
    directly, with human-readable label, kind and severity from the bundled reference.
    """  # noqa: DAR101,DAR201
    err = config_missing("dwc", response_format)
    if err is not None:
        return err
    try:
        dtbegin = to_datetime(dtbegin)
        dtend = to_datetime(dtend)
        df = read_alerts(patientids, dtbegin, dtend)
        return dataframe_to_response(df, response_format, max_rows=max_rows)
    except Exception as exc:  # noqa: BLE001
        return handle_error(exc, response_format)


# --------------------------------------------------------------------------- #
# Code-generation reference resource
# --------------------------------------------------------------------------- #
_REFERENCE_FUNCS = [
    read_patients,
    read_patient,
    read_patients_dwc_native,
    read_patient_dwc_native,
    read_numerics,
    read_waves,
    read_wave_chunks,
    read_enumerations,
    read_alerts,
]

_REFERENCE_HEADER = '''\
# dwclib code-generation reference

Use the MCP tools to *discover* patients and what data they have, then write Python
that calls these ``dwclib`` functions directly to pull the bulk data. Waveform and
numerics queries can return millions of samples, so keep time windows narrow and pull
raw data in your own code rather than through MCP.

Time bounds are half-open: ``dtbegin <= TimeStamp < dtend``. ``dtbegin``/``dtend``
must be ``datetime`` objects. Connection URIs come from the server-side config
file, so omit the ``uri`` argument.

End-to-end example::

    from datetime import datetime
    from dwclib import read_patients, read_numerics, read_waves

    # 1. Discover a patient and the labels/time bounds available for them.
    patients = read_patients(bedlabel="ICU-3",
                             dtbegin=datetime(2021, 1, 1), dtend=datetime(2021, 1, 2))
    pid = patients.index[0]

    # 2. Pull numerics for the discovered labels and window.
    nums = read_numerics(pid, datetime(2021, 1, 1, 8), datetime(2021, 1, 1, 9),
                         labels=["HR", "NBP"])

    # 3. Pull a narrow waveform window.
    waves = read_waves(pid, datetime(2021, 1, 1, 8), datetime(2021, 1, 1, 8, 1),
                       labels=["II"])

## Function reference
'''


def _build_reference() -> str:
    parts = [_REFERENCE_HEADER]
    for func in _REFERENCE_FUNCS:
        signature = str(inspect.signature(func))
        doc = inspect.getdoc(func) or "(no docstring)"
        parts.append(f"\n### `{func.__name__}{signature}`\n\n{doc}\n")
    return "\n".join(parts)


@mcp.resource(
    "dwclib://reference",
    name="dwclib API reference",
    description=(
        "Signatures and docstrings of the public dwclib read_* functions, with an "
        "end-to-end example, for generating code that pulls bulk data."
    ),
    mime_type="text/markdown",
)
def dwclib_reference() -> str:
    """Return the public dwclib API reference for code generation."""
    return _build_reference()


def main() -> None:
    """Console-script entry point: run the MCP server over stdio."""
    mcp.run()


if __name__ == "__main__":
    main()
