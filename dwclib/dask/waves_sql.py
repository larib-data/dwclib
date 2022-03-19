from datetime import timedelta
from itertools import count

import pandas as pd
from dwclib.common.waves_query import build_waves_query
from sqlalchemy import create_engine

import dask.dataframe as dd
from dask import delayed
from dask.dataframe.utils import make_meta


def waves_meta():
    idx = pd.DatetimeIndex([], name='TimeStamp')
    dtypes = {
        'PatientId': 'string',
        'Label': 'string',
        'WaveSamples': 'bytes',
        'SamplePeriod': 'int32',
        'CAU': 'float64',
        'CAL': 'float64',
        'CSU': 'int32',
        'CSL': 'int32',
    }
    mdf = pd.DataFrame({k: [] for k in dtypes.keys()}, index=idx)
    mdf = mdf.astype(dtype=dtypes)
    return make_meta(mdf)


def build_divisions(dtbegin, dtend, interval):
    ranges = []
    for i in count():
        beg = dtbegin + i * interval
        end = beg + interval
        ranges.append((beg, end))
        if end >= dtend:
            break
    divisions = [beg for beg, _ in ranges]
    divisions.append(dtend)
    return (ranges, divisions)


def run_query(uri, dfmeta, dtbegin, dtend, patientid, labels=[]):
    engine = create_engine(uri)
    q = build_waves_query(engine, dtbegin, dtend, patientid, labels)

    with engine.connect() as conn:
        df = pd.read_sql(q, conn, index_col='TimeStamp')
    engine.dispose()
    if len(df) == 0:
        return dfmeta
    else:
        df.index = pd.to_datetime(df.index, utc=True).to_numpy(dtype='datetime64[ns]')
        return df.astype(dfmeta.dtypes.to_dict(), copy=False)


def read_wave_chunks(
    patientid,
    dtbegin,
    dtend,
    uri,
    labels=[],
    interval=timedelta(hours=1),
):
    ranges, divisions = build_divisions(dtbegin, dtend, interval)
    meta = waves_meta()
    parts = []
    for begin, end in ranges:
        parts.append(
            delayed(run_query)(
                uri,
                meta,
                begin,
                end,
                patientid,
                labels,
            )
        )
    return dd.from_delayed(parts, meta, divisions=divisions)
