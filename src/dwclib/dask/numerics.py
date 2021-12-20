from datetime import timedelta
from itertools import count

import dask.dataframe as dd
import pandas as pd
from dask import delayed
from dask.dataframe.utils import make_meta
from sqlalchemy import MetaData
from sqlalchemy import Table
from sqlalchemy import and_
from sqlalchemy import column
from sqlalchemy import create_engine
from sqlalchemy import select


def build_metas():
    df_numerics = pd.DataFrame({'SubLabel': []}, index=pd.Int64Index([], name='Id'))
    df_numerics = df_numerics.astype(dtype={"SubLabel": "string"})
    meta_numerics = make_meta(df_numerics)
    df_numeric_values = pd.DataFrame(
        {'PatientId': [], 'NumericId': [], 'Value': []},
        index=pd.DatetimeIndex([], name='TimeStamp'),
    )
    df_numeric_values = df_numeric_values.astype(
        dtype={"PatientId": "string", 'NumericId': 'int64', 'Value': 'float32'}
    )
    meta_numeric_values = make_meta(df_numeric_values)
    return (meta_numerics, meta_numeric_values)


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


def run_numerics_query(uri, dfmeta, dtbegin, dtend):
    engine = create_engine(uri)
    dbmeta = MetaData(bind=engine)
    nnt = Table(
        'External_Numeric', dbmeta, schema='dbo', autoload=True, autoload_with=engine
    )
    q = select(nnt.c.Id, nnt.c.SubLabel)
    q = q.where(nnt.c.TimeStamp >= dtbegin)
    q = q.where(nnt.c.TimeStamp < dtend)
    q = q.where(nnt.c.SubLabel is not None)
    with engine.connect() as conn:
        df = pd.read_sql(q, conn, index_col='Id')
    engine.dispose()
    if len(df) == 0:
        return dfmeta
    else:
        return df.astype(dfmeta.dtypes.to_dict(), copy=False)


def run_numeric_values_query(uri, dfmeta, pid, dtbegin, dtend):
    engine = create_engine(uri)
    dbmeta = MetaData(bind=engine)
    nvt = Table(
        'External_NumericValue',
        dbmeta,
        schema='dbo',
        autoload=True,
        autoload_with=engine,
    )
    q = select(nvt.c.PatientId, nvt.c.NumericId, nvt.c.TimeStamp, nvt.c.Value)
    q = q.where(nvt.c.TimeStamp >= dtbegin)
    q = q.where(nvt.c.TimeStamp < dtend)
    q = q.where(nvt.c.PatientId == pid)
    q = q.where(nvt.c.Value is not None)
    with engine.connect() as conn:
        df = pd.read_sql(q, conn, index_col='TimeStamp')
        df.index = df.index.astype('datetime64[ns]')
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
    interval=timedelta(hours=1),
    query_hook=None,
    engine_kwargs=None,
    **kwargs
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
    ddf = nvdd.join(nndd, on='NumericId', how='inner')
    return ddf
