"""Shared formatting and summarisation helpers for the dwclib MCP server.

These helpers convert the pandas frames returned by the public ``read_*``
functions into the compact, JSON- or Markdown-friendly payloads the MCP tools
hand back to the agent. Nothing here imports :mod:`fastmcp`, so the module can be
imported (and unit-tested) without the optional ``mcp`` extra installed.
"""

from datetime import datetime
from enum import Enum
from typing import List, Union

import numpy as np
import pandas as pd

DEFAULT_MAX_ROWS = 200


class ResponseFormat(str, Enum):
    """Output format accepted by the convenience tools."""

    json = "json"
    markdown = "markdown"


def _jsonable(value):
    """Convert a single cell value into a JSON-serialisable Python object."""
    if value is None:
        return None
    # Scalars: pd.isna raises on array-likes, so guard it.
    try:
        if pd.isna(value):
            return None
    except (TypeError, ValueError):
        pass
    if isinstance(value, (pd.Timestamp, datetime)):
        return value.isoformat()
    if isinstance(value, np.generic):
        return value.item()
    if isinstance(value, dict):
        return {str(k): _jsonable(v) for k, v in value.items()}
    if isinstance(value, (list, tuple, set, np.ndarray, pd.Series)):
        return [_jsonable(v) for v in value]
    return value


def _md_cell(value) -> str:
    """Render a cell value for a Markdown table, escaping pipes."""
    conv = _jsonable(value)
    if conv is None:
        return ""
    return str(conv).replace("|", "\\|").replace("\n", " ")


def _markdown_table(
    df: pd.DataFrame,
    columns: List[str],
    total: int,
    returned: int,
    truncated: bool,
) -> str:
    header = "| " + " | ".join(columns) + " |"
    sep = "| " + " | ".join(["---"] * len(columns)) + " |"
    lines = [header, sep]
    for row in df.itertuples(index=False, name=None):
        lines.append("| " + " | ".join(_md_cell(v) for v in row) + " |")
    note = f"_{returned} of {total} row(s) shown"
    note += " (truncated)._" if truncated else "._"
    if not len(df):
        lines = [header, sep]
    return "\n".join(lines) + "\n\n" + note


def dataframe_to_response(
    df: pd.DataFrame,
    response_format: Union[str, ResponseFormat] = ResponseFormat.json,
    max_rows: int = DEFAULT_MAX_ROWS,
):
    """Convert a dataframe into a tool response payload.

    Args:
        df: The dataframe to serialise. Its index is reset into a leading column.
        response_format: ``json`` (default) for a structured dict, or ``markdown``
            for a rendered table string.
        max_rows: Maximum number of rows to include; extra rows are dropped and the
            response is flagged as truncated.

    Returns:
        Either a dict with ``columns``, ``total_rows``, ``returned``, ``truncated``
        and ``records`` keys (JSON), or a Markdown table string.
    """
    response_format = ResponseFormat(response_format)
    total = int(len(df))
    truncated = total > max_rows
    shown = df.head(max_rows) if truncated else df
    # Keep a meaningful index (patientid, begin timestamp, ...) as a leading column,
    # but drop the anonymous RangeIndex the summary frames carry.
    drop_index = isinstance(shown.index, pd.RangeIndex) and shown.index.name is None
    disp = shown.reset_index(drop=drop_index)
    columns = [str(c) for c in disp.columns]
    returned = int(len(disp))
    if response_format is ResponseFormat.markdown:
        return _markdown_table(disp, columns, total, returned, truncated)
    records = [
        {col: _jsonable(val) for col, val in zip(columns, row, strict=False)}
        for row in disp.itertuples(index=False, name=None)
    ]
    return {
        "columns": columns,
        "total_rows": total,
        "returned": returned,
        "truncated": truncated,
        "records": records,
    }


def summarize_numeric(df: pd.DataFrame) -> pd.DataFrame:
    """Summarise a long-form numerics frame per ``(PatientId, Label, SubLabel)``.

    Args:
        df: The long frame returned by ``read_numerics(pivot=False)`` (indexed by
            ``TimeStamp`` with ``PatientId``, ``Label``, ``SubLabel``, ``Value``).

    Returns:
        A dataframe with one row per signal holding ``count``, ``min``, ``max``,
        ``mean``, ``std`` and the ``first``/``last`` sample timestamps.
    """
    cols = [
        "PatientId", "Label", "SubLabel",
        "count", "min", "max", "mean", "std", "first", "last",
    ]
    if df.empty:
        return pd.DataFrame(columns=cols)
    g = df.reset_index()
    keys = ["PatientId", "Label", "SubLabel"]
    stats = g.groupby(keys, dropna=False)["Value"].agg(
        ["count", "min", "max", "mean", "std"]
    )
    times = g.groupby(keys, dropna=False)["TimeStamp"].agg(first="min", last="max")
    return stats.join(times).reset_index()[cols]


def summarize_categorical(df: pd.DataFrame) -> pd.DataFrame:
    """Summarise a long-form enumerations frame per ``(PatientId, Label)``.

    Args:
        df: The long frame returned by ``read_enumerations(pivot=False)`` (indexed
            by ``TimeStamp`` with ``PatientId``, ``Label``, ``Value``).

    Returns:
        A dataframe with one row per parameter holding the ``distinct`` value count,
        total ``count``, a ``value_counts`` mapping and the ``first``/``last``
        timestamps.
    """
    cols = [
        "PatientId", "Label", "distinct", "count",
        "value_counts", "first", "last",
    ]
    if df.empty:
        return pd.DataFrame(columns=cols)
    g = df.reset_index()
    rows = []
    for (pid, label), sub in g.groupby(["PatientId", "Label"], dropna=False):
        values = sub["Value"]
        rows.append({
            "PatientId": pid,
            "Label": label,
            "distinct": int(values.nunique()),
            "count": int(values.count()),
            "value_counts": values.value_counts().to_dict(),
            "first": sub["TimeStamp"].min(),
            "last": sub["TimeStamp"].max(),
        })
    return pd.DataFrame(rows, columns=cols)


def _error_message(exc: Exception) -> str:
    """Turn an exception into a short, actionable message."""
    from dwclib.common import db

    name = type(exc).__name__
    text = str(exc)
    lowered = text.lower()
    if "connect" in lowered or "timeout" in lowered or "login" in lowered:
        return (
            f"Could not reach the database ({name}: {text}). Check that the DWC / "
            f"DWCmeta servers are reachable and that the credentials in "
            f"{db.configfile} are correct."
        )
    return f"{name}: {text}"


def config_missing(kind: str, response_format: Union[str, ResponseFormat]):
    """Return an error payload if the required database is not configured.

    Args:
        kind: ``"dwc"`` for the MSSQL DWC database or ``"pg"`` for the PostgreSQL
            DWCmeta database.
        response_format: The format to render the error in.

    Returns:
        An error payload (dict or string) when the connection URI is missing, or
        ``None`` when configuration is present.
    """
    from dwclib.common import db

    uri = db.pguri if kind == "pg" else db.dwcuri
    if uri is not None:
        return None
    label = "DWCmeta (PostgreSQL)" if kind == "pg" else "DWC (MSSQL)"
    msg = (
        f"No {label} database is configured. Edit {db.configfile} or call "
        f"dwclib.common.db.update_config(...) to set the connection, then restart "
        f"the server."
    )
    return _wrap_error(msg, response_format)


def handle_error(exc: Exception, response_format: Union[str, ResponseFormat]):
    """Render an exception as a graceful tool response instead of a stack trace."""
    return _wrap_error(_error_message(exc), response_format)


def _wrap_error(msg: str, response_format: Union[str, ResponseFormat]):
    if ResponseFormat(response_format) is ResponseFormat.markdown:
        return f"**Error:** {msg}"
    return {"error": msg}
