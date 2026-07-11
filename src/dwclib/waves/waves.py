from collections import defaultdict
from datetime import datetime
from multiprocessing.pool import ThreadPool
from typing import List, Optional, Union

import pandas as pd

from dwclib.common.db import dwcuri
from dwclib.common.meta import waves_meta
from dwclib.common.wave_unfold import unfold_row
from dwclib.common.waves import run_waves_query


def read_waves(
    patientid: str,
    dtbegin: Union[str, datetime],
    dtend: Union[str, datetime],
    labels: Optional[List[str]] = None,
    uri: str = None,
) -> pd.DataFrame:
    """Reads high-frequency waveforms from the DWC database.

    Retrieves sampled waveform signals (e.g. ECG, arterial pressure,
    plethysmogram) for a single patient and unfolds the packed samples into a
    regularly-sampled frame. A single query can return millions of samples, so
    keep the time window narrow.

    Args:
        patientid: A DWC patient identifier.
        dtbegin: Start of the time window (inclusive), as an ISO-8601 string or datetime.
        dtend: End of the time window (exclusive), as an ISO-8601 string or datetime.
        labels: Optional list of waveform labels to restrict the query
            (e.g. ``["II", "Pleth"]``). Empty or None returns all labels.
        uri: Optional sqlalchemy URI for the database if not provided in the config file.

    Returns:
        A pandas dataframe indexed by UTC timestamp with one column per waveform label,
        each holding the unfolded sample values. Only samples with
        ``dtbegin <= TimeStamp < dtend`` are returned. If no rows match, an empty frame
        with the waves dtypes is returned.

    Examples:
        Fetch the lead II ECG for a single patient over one minute::

            from dwclib import read_waves

            df = read_waves(
                "abcd1234-ef56-7890-abcd-ef1234567890",
                "2021-01-01T00:00:00",
                "2021-01-01T00:01:00",
                labels=["II"],
            )
    """
    if not uri:
        uri = dwcuri
    if labels is None:
        labels = []
    df = run_waves_query(uri, dtbegin, dtend, patientid, labels)
    return unfold_pandas_dataframe(df)


def read_wave_chunks(
    patientid: str,
    dtbegin: Union[str, datetime],
    dtend: Union[str, datetime],
    labels: Optional[List[str]] = None,
    uri: str = None,
) -> pd.DataFrame:
    """Reads raw waveform chunks from the DWC database without unfolding.

    Returns the waveform rows exactly as stored, one row per chunk with the packed
    ``WaveSamples`` and their calibration metadata, leaving the caller to unfold them.
    Useful when you want to inspect or process the packed representation directly.

    Args:
        patientid: A DWC patient identifier.
        dtbegin: Start of the time window (inclusive), as an ISO-8601 string or datetime.
        dtend: End of the time window (exclusive), as an ISO-8601 string or datetime.
        labels: Optional list of waveform labels to restrict the query
            (e.g. ``["II", "Pleth"]``). Empty or None returns all labels.
        uri: Optional sqlalchemy URI for the database if not provided in the config file.

    Returns:
        A pandas dataframe indexed by UTC timestamp with columns ``PatientId``, ``Label``,
        ``WaveSamples`` (the packed samples), ``SamplePeriod`` and the calibration bounds
        (``CAU``, ``CAL``, ``CSU``, ``CSL``). Only chunks with
        ``dtbegin <= TimeStamp < dtend`` are returned.

    Examples:
        Fetch the raw packed chunks for a single patient::

            from dwclib import read_wave_chunks

            df = read_wave_chunks(
                "abcd1234-ef56-7890-abcd-ef1234567890",
                "2021-01-01T00:00:00",
                "2021-01-01T00:01:00",
                labels=["II"],
            )
    """
    if not uri:
        uri = dwcuri
    if labels is None:
        labels = []
    return run_waves_query(uri, dtbegin, dtend, patientid, labels)


def unfold_pandas_dataframe(df: pd.DataFrame) -> pd.DataFrame:
    # TODO: see if we can efficiently pre-allocate buffer
    databuffer = defaultdict(list)
    for _, row in df.iterrows():
        srow = unfold_row(row)
        databuffer[row["Label"]].append(srow)
    if not databuffer:
        return waves_meta
    concatter = dictconcatter(databuffer)
    with ThreadPool() as pool:
        pool.map(concatter, databuffer.keys())
    return pd.DataFrame(databuffer)


def dictconcatter(d) -> pd.DataFrame:
    def dictconcat_runner(k):
        d[k] = pd.concat(d[k], axis=0, copy=False).groupby(level=0).max()

    return dictconcat_runner
