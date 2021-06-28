from datetime import timedelta
from itertools import count

import dask.dataframe as dd
import numpy as np
import pandas as pd
from dask import delayed
from sqlalchemy import and_
from sqlalchemy import column
from sqlalchemy import create_engine

from dwclib.numerics import build_numerics_query


def read_numerics(
    patientid,
    dtbegin,
    dtend,
    uri,
    interval=timedelta(hours=1),
    query_hook=None,
    engine_kwargs=None,
    **kwargs
):
    ranges = []
    for i in count():
        beg = dtbegin + i * interval
        end = beg + interval
        ranges.append((beg, end))
        if end >= dtend:
            break
    divisions = [beg for beg, _ in ranges]
    divisions.append(dtend)

    meta = get_numeric_meta()
    parts = []
    for begin, end in ranges:
        parts.append(
            delayed(_read_sql_chunk)(
                patientid,
                begin,
                end,
                uri,
                meta,
                query_hook,
                engine_kwargs=None,
                **kwargs
            )
        )
    return dd.from_delayed(parts, meta, divisions=divisions)


def _read_sql_chunk(
    patientid,
    dtbegin,
    dtend,
    uri,
    meta,
    query_hook=None,
    engine_kwargs=None,
    **kwargs
):
    engine = create_engine(uri)
    q = build_numerics_query(dtbegin, dtend, patientid)
    if query_hook:
        q = query_hook(q)
    with engine.connect() as conn:
        df = pd.read_sql(q, conn, index_col='DateTime')
    engine.dispose()
    df = df.dropna(axis=0, how='any', subset=['Value'])
    df.index = pd.to_datetime(df.index)
    if len(df) == 0:
        return meta
    else:
        return df.astype(meta.dtypes.to_dict(), copy=False)


def get_numeric_meta():
    index = pd.DatetimeIndex([], name='DateTime')
    meta = pd.DataFrame(
        columns=[
            'PatientId',
            'Label',
            'Value',
        ],
        index=index,
    )
    meta['PatientId'] = meta['PatientId'].astype('string')
    meta['Label'] = meta['Label'].astype('string')
    meta['Value'] = meta['Value'].astype(np.float32)
    return meta
