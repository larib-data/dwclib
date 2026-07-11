from datetime import datetime
from typing import List, Optional, Union

import pandas as pd
from pandas.api.types import is_list_like

from dwclib.common.db import dwcuri
from dwclib.common.meta import numerics_meta
from dwclib.common.numerics import run_numerics_query


def read_numerics(
    patientids: Union[None, str, List[str]],
    dtbegin: datetime,
    dtend: datetime,
    labels: Optional[List[str]] = None,
    sublabels: Optional[List[str]] = None,
    pivot: bool = True,
    uri: Optional[str] = None,
) -> pd.DataFrame:
    """Reads numeric parameters from the DWC database.

    Retrieves periodic numeric measurements (e.g. heart rate, blood pressure,
    SpO2) for one or several patients over a time window.

    Args:
        patientids: A DWC patient identifier or list of identifiers. Pass None to
            retrieve every patient with data in the window. A single-element list is
            unwrapped and treated as a single patient.
        dtbegin: Start of the time window (inclusive), as a datetime.
        dtend: End of the time window (exclusive), as a datetime.
        labels: Optional list of numeric labels to restrict the query
            (e.g. ``["HR", "NBP"]``). Empty or None returns all labels.
        sublabels: Optional list of numeric sublabels to restrict the query
            (e.g. ``["Systolic", "Diastolic"]``). Empty or None returns all sublabels.
        pivot: If True (default), return a wide frame indexed by timestamp with one
            column per signal. If False, return the long frame with columns
            ``PatientId``, ``Label``, ``SubLabel`` and ``Value``.
        uri: Optional sqlalchemy URI for the database if not provided in the config file.

    Returns:
        A pandas dataframe indexed by UTC timestamp. When ``pivot`` is True the columns
        are a ``(PatientId, SubLabel)`` MultiIndex, with the ``PatientId`` level dropped
        when a single patient was requested. When ``pivot`` is False the frame is in long
        form. Only samples with ``dtbegin <= TimeStamp < dtend`` are returned. If no rows
        match, an empty frame with the numerics dtypes is returned.

    Examples:
        Fetch two signals for a single patient as a pivoted frame::

            from datetime import datetime
            from dwclib import read_numerics

            df = read_numerics(
                "abcd1234-ef56-7890-abcd-ef1234567890",
                datetime(2021, 1, 1),
                datetime(2021, 1, 1, 1),
                labels=["HR", "NBP"],
            )

        Retrieve the long-form frame for downstream aggregation::

            df = read_numerics(
                "abcd1234-ef56-7890-abcd-ef1234567890",
                datetime(2021, 1, 1),
                datetime(2021, 1, 1, 1),
                pivot=False,
            )
    """
    if not uri:
        uri = dwcuri
    if labels is None:
        labels = []
    if sublabels is None:
        sublabels = []
    if is_list_like(patientids) and len(patientids) == 1:
        patientids = patientids[0]
    df = run_numerics_query(uri, dtbegin, dtend, patientids, labels, sublabels)
    df = df.dropna(axis=0, how="any", subset=["Value"])
    if not len(df.index):
        return numerics_meta
    if pivot:
        df = pivot_numerics(df)
    single_patient = patientids is not None and not is_list_like(patientids)
    if single_patient:
        df.columns = df.columns.droplevel(0)
    # Other way to check if there is a single patientid:
    # if len(df.columns.get_level_values(0).drop_duplicates()) == 1:
    return df


def pivot_numerics(df: pd.DataFrame) -> Optional[pd.DataFrame]:
    df = df.pivot_table(
        index=df.index,
        columns=["PatientId", "SubLabel"],
        values="Value",
        aggfunc="max",
    )
    return df
