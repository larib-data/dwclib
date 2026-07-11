from datetime import datetime
from typing import List, Optional, Union

import pandas as pd
from pandas.api.types import is_list_like
from sqlalchemy import MetaData, Table, create_engine, join, select

from dwclib.common.db import dwcuri
from dwclib.common.meta import enumerations_meta


def read_enumerations(
    patientids: Union[None, str, List[str]],
    dtbegin: datetime,
    dtend: datetime,
    labels: Optional[List[str]] = None,
    pivot: bool = True,
    uri: Optional[str] = None,
) -> pd.DataFrame:
    """Reads enumerations (categorical parameters) from the DWC database.

    Retrieves categorical / textual monitor parameters (e.g. rhythm labels,
    ventilation modes) for one or several patients over a time window.

    Args:
        patientids: A DWC patient identifier or list of identifiers. Pass None to
            retrieve every patient with data in the window. A single-element list is
            unwrapped and treated as a single patient.
        dtbegin: Start of the time window (inclusive), as a datetime.
        dtend: End of the time window (exclusive), as a datetime.
        labels: Optional list of enumeration labels to restrict the query. Empty or None
            returns all labels.
        pivot: If True (default), return a wide frame indexed by timestamp with one
            column per signal. If False, return the long frame with columns
            ``PatientId``, ``Label`` and ``Value``.
        uri: Optional sqlalchemy URI for the database if not provided in the config file.

    Returns:
        A pandas dataframe indexed by UTC timestamp. When ``pivot`` is True the columns
        are a ``(PatientId, Label)`` MultiIndex, with the ``PatientId`` level dropped when
        a single patient was requested. When ``pivot`` is False the frame is in long form.
        Only samples with ``dtbegin <= TimeStamp < dtend`` are returned. If no rows match,
        an empty frame with the enumerations dtypes is returned.

    Examples:
        Fetch enumerations for a single patient in long form::

            from datetime import datetime
            from dwclib import read_enumerations

            df = read_enumerations(
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
    if is_list_like(patientids) and len(patientids) == 1:
        patientids = patientids[0]
    df = run_enumerations_query(uri, dtbegin, dtend, patientids, labels)
    df = df.dropna(axis=0, how="any", subset=["Value"])
    if not len(df.index):
        return enumerations_meta
    if pivot:
        df = pivot_enumerations(df)
    single_patient = patientids is not None and not is_list_like(patientids)
    if pivot and single_patient:
        df.columns = df.columns.droplevel(0)
    return df


def pivot_enumerations(df: pd.DataFrame) -> Optional[pd.DataFrame]:
    df = df.pivot_table(
        index=df.index,
        columns=["PatientId", "Label"],
        values="Value",
        aggfunc="max",
    )
    return df


def run_enumerations_query(
    uri: str,
    dtbegin: datetime,
    dtend: datetime,
    patientids: Union[None, str, List[str]],
    labels: List[str],
) -> pd.DataFrame:
    engine = create_engine(uri)
    q = build_enumerations_query(engine, dtbegin, dtend, patientids, labels)

    with engine.connect() as conn:
        df = pd.read_sql(q, conn, index_col="TimeStamp")
    engine.dispose()
    if len(df) == 0:
        return enumerations_meta
    else:
        df.index = pd.to_datetime(df.index, utc=True)
        return df.astype(enumerations_meta.dtypes.to_dict(), copy=False)


def build_enumerations_query(engine, dtbegin, dtend, patientids, labels):
    dbmeta = MetaData(schema="_Export")
    eet = Table("Enumeration_", dbmeta, autoload_with=engine)
    evt = Table("EnumerationValue_", dbmeta, autoload_with=engine)

    ee = select(eet.c.TimeStamp, eet.c.Id, eet.c.Label)
    ee = ee.with_hint(eet, "WITH (NOLOCK)")
    ee = ee.where(eet.c.TimeStamp >= dtbegin)
    ee = ee.where(eet.c.TimeStamp < dtend)
    if labels:
        ee = ee.where(eet.c.Label.in_(labels))
    ee = ee.cte("Enumeration")

    ev = select(evt.c.TimeStamp, evt.c.EnumerationId, evt.c.Value, evt.c.PatientId)
    ev = ev.with_hint(evt, "WITH (NOLOCK)")
    ev = ev.where(evt.c.TimeStamp >= dtbegin)
    ev = ev.where(evt.c.TimeStamp < dtend)
    ev = ev.where(evt.c.Value.is_not(None))
    if patientids:
        if is_list_like(patientids):
            ev = ev.where(evt.c.PatientId.in_(patientids))
        else:
            ev = ev.where(evt.c.PatientId == patientids)
    ev = ev.cte("EnumerationValue")

    j = join(ev, ee, ev.c.EnumerationId == ee.c.Id)
    q = select(ev.c.TimeStamp, ev.c.PatientId, ee.c.Label, ev.c.Value).select_from(j)
    return q
