from datetime import timedelta
from itertools import count

import dask.dataframe as dd
import pandas as pd
from dask import delayed
from dask.dataframe.utils import make_meta
from sqlalchemy import MetaData
from sqlalchemy import Table
from sqlalchemy import create_engine
from sqlalchemy import join
from sqlalchemy import select


def build_query(engine, dtbegin, dtend, patientids=[], labels=[]):
    dbmeta = MetaData(bind=engine)
    nnt = Table(
        'Numeric_', dbmeta, schema='_Export', autoload=True, autoload_with=engine
    )
    nvt = Table(
        'NumericValue_', dbmeta, schema='_Export', autoload=True, autoload_with=engine
    )

    nn = select(nnt.c.TimeStamp, nnt.c.Id, nnt.c.SubLabel)
    nn = nn.where(nnt.c.TimeStamp >= dtbegin)
    nn = nn.where(nnt.c.TimeStamp < dtend)
    if labels:
        nn = nn.where(nnt.c.SubLabel.in_(labels))
    nn = nn.cte('Numeric')

    nv = select(nvt.c.TimeStamp, nvt.c.NumericId, nvt.c.Value, nvt.c.PatientId)
    nv = nv.where(nvt.c.TimeStamp >= dtbegin)
    nv = nv.where(nvt.c.TimeStamp < dtend)
    nv = nv.where(nvt.c.Value is not None)
    if patientids:
        nv = nv.where(nvt.c.PatientId.in_(patientids))
    nv = nv.cte('NumericValue')

    j = join(nv, nn, nv.c.NumericId == nn.c.Id)
    q = select(nv.c.TimeStamp, nv.c.PatientId, nn.c.SubLabel, nv.c.Value).select_from(j)
    return q


def build_meta():
    idx = pd.DatetimeIndex([], name='TimeStamp')
    dtypes = {
        'PatientId': 'string',
        'SubLabel': 'string',
        'Value': 'float32',
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


def run_query(uri, dfmeta, dtbegin, dtend, patientids=[], labels=[]):
    engine = create_engine(uri)
    q = build_query(engine, dtbegin, dtend, patientids, labels)

    with engine.connect() as conn:
        df = pd.read_sql(q, conn, index_col='TimeStamp')
    engine.dispose()
    if len(df) == 0:
        return dfmeta
    else:
        df.index = pd.to_datetime(df.index, utc=True).to_numpy(dtype='datetime64[ns]')
        return df.astype(dfmeta.dtypes.to_dict(), copy=False)


def read_numerics(
    patientids,
    dtbegin,
    dtend,
    uri,
    labels=[],
    interval=timedelta(hours=1),
):
    ranges, divisions = build_divisions(dtbegin, dtend, interval)
    meta = build_meta()
    parts = []
    for begin, end in ranges:
        parts.append(
            delayed(run_query)(
                uri,
                meta,
                begin,
                end,
                patientids,
                labels,
            )
        )
    return dd.from_delayed(parts, meta, divisions=divisions)
