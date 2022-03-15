from datetime import timedelta
from itertools import count

import dask.dataframe as dd
import pandas as pd
from dask import delayed
from dask.dataframe.utils import make_meta
from sqlalchemy import MetaData
from sqlalchemy import Table
from sqlalchemy import create_engine
from sqlalchemy import select


def build_metas():
    idx = pd.DatetimeIndex([], name='TimeStamp')
    nums_dtypes = {
        'Id': 'int64',
        'SubLabel': 'string',
    }
    num_values_dtypes = {
        'PatientId': 'string',
        'NumericId': 'int64',
        'Value': 'float32',
    }
    df_nums = pd.DataFrame({k: [] for k in nums_dtypes.keys()}, index=idx)
    df_nums = df_nums.astype(dtype=nums_dtypes)
    meta_nums = make_meta(df_nums)

    df_num_values = pd.DataFrame({k: [] for k in num_values_dtypes.keys()}, index=idx)
    df_num_values = df_num_values.astype(dtype=num_values_dtypes)
    meta_num_values = make_meta(df_num_values)
    return (meta_nums, meta_num_values)


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


def run_numerics_query(uri, dfmeta, dtbegin, dtend, labels=[]):
    engine = create_engine(uri)
    dbmeta = MetaData(bind=engine)
    nnt = Table(
        'Numeric_', dbmeta, schema='_Export', autoload=True, autoload_with=engine
    )
    q = select(nnt.c.Id, nnt.c.SubLabel, nnt.c.TimeStamp)
    q = q.where(nnt.c.TimeStamp >= dtbegin)
    q = q.where(nnt.c.TimeStamp < dtend)
    q = q.where(nnt.c.SubLabel is not None)
    if labels:
        q = q.where(nnt.c.SubLabel.in_(labels))

    with engine.connect() as conn:
        df = pd.read_sql(q, conn, index_col='TimeStamp')
    engine.dispose()
    if len(df) == 0:
        return dfmeta
    else:
        df.index = pd.to_datetime(df.index, utc=True).to_numpy(dtype='datetime64[ns]')
        return df.astype(dfmeta.dtypes.to_dict(), copy=False)


def run_numeric_values_query(uri, dfmeta, pid, dtbegin, dtend):
    engine = create_engine(uri)
    dbmeta = MetaData(bind=engine)
    nvt = Table(
        'NumericValue_', dbmeta, schema='_Export', autoload=True, autoload_with=engine
    )
    q = select(nvt.c.PatientId, nvt.c.NumericId, nvt.c.TimeStamp, nvt.c.Value)
    q = q.where(nvt.c.TimeStamp >= dtbegin)
    q = q.where(nvt.c.TimeStamp < dtend)
    q = q.where(nvt.c.PatientId == pid)
    q = q.where(nvt.c.Value is not None)
    with engine.connect() as conn:
        df = pd.read_sql(q, conn, index_col='TimeStamp')
        df = df.tz_localize(None)
    engine.dispose()
    if len(df) == 0:
        return dfmeta
    else:
        return df.astype(dfmeta.dtypes.to_dict(), copy=False)


def read_numerics(
    patientid,
    dtbegin,
    dtend,
    uri,
    labels=[],
    interval=timedelta(hours=1),
):
    ranges, divisions = build_divisions(dtbegin, dtend, interval)
    meta_numerics, meta_numeric_values = build_metas()

    numerics_parts = []
    numeric_values_parts = []
    for begin, end in ranges:
        numerics_parts.append(
            delayed(run_numerics_query)(
                uri,
                meta_numerics,
                begin,
                end,
                labels,
            )
        )
        numeric_values_parts.append(
            delayed(run_numeric_values_query)(
                uri,
                meta_numeric_values,
                patientid,
                begin,
                end,
            )
        )
    nndd = dd.from_delayed(numerics_parts, meta_numerics, divisions=divisions)
    nvdd = dd.from_delayed(
        numeric_values_parts, meta_numeric_values, divisions=divisions
    )
    nndd = nndd.set_index('Id')
    ddf = nvdd.join(nndd, on='NumericId', how='inner')
    ddf = ddf.drop('NumericId', axis=1)
    return ddf
