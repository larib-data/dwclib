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
    engine_kwargs=None,
    **kwargs
):
    ranges = []
    for i in count():
        beg = dtbegin + i * interval
        end = beg + interval - timedelta(milliseconds=1)
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
                begin, end, patientid, uri, meta, engine_kwargs=None, **kwargs
            )
        )
    return dd.from_delayed(parts, meta, divisions=divisions)


def _read_sql_chunk(
    dtbegin, dtend, patientid, uri, meta, engine_kwargs=None, **kwargs
):
    engine = create_engine(uri)
    q = build_numerics_query(dtbegin, dtend, patientid)
    with engine.connect() as conn:
        df = pd.read_sql(q, conn, index_col='DateTime')
    engine.dispose()
    df = df.dropna(axis=0, how='any', subset=['Value'])
    #df['Value'] = df['Value'].astype('float32')

    if len(df) == 0:
        return meta
    elif len(meta.dtypes.to_dict()) == 0:
        # only index column in loaded
        # required only for pandas < 1.0.0
        return df
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
    meta['PatientId'] = meta['PatientId'].astype(object)
    meta['Label'] = meta['Label'].astype(object)
    meta['Value'] = meta['Value'].astype(float)
    # return dd.utils.make_meta(meta, index=index)
    return meta
