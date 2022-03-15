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


def build_query(engine, dtbegin, dtend, patientid, labels=[]):
    dbmeta = MetaData(bind=engine)
    wwt = Table('Wave_', dbmeta, schema='_Export', autoload=True, autoload_with=engine)
    wst = Table(
        'WaveSample_', dbmeta, schema='_Export', autoload=True, autoload_with=engine
    )

    ww = select(
        wwt.c.TimeStamp,
        wwt.c.Id,
        wwt.c.Label,
        wwt.c.SamplePeriod,
        wwt.c.CalibrationAbsUpper.label('CAU'),
        wwt.c.CalibrationAbsLower.label('CAL'),
        wwt.c.CalibrationScaledUpper.label('CSU'),
        wwt.c.CalibrationScaledLower.label('CSL'),
    )
    ww = ww.where(wwt.c.TimeStamp >= dtbegin)
    ww = ww.where(wwt.c.TimeStamp < dtend)
    if labels:
        ww = ww.where(wwt.c.Label.in_(labels))
    ww = ww.cte('Wave')

    ws = select(wst.c.TimeStamp, wst.c.WaveId, wst.c.WaveSamples, wst.c.PatientId)
    ws = ws.where(wst.c.TimeStamp >= dtbegin)
    ws = ws.where(wst.c.TimeStamp < dtend)
    ws = ws.where(wst.c.WaveSamples is not None)
    if patientid:
        ws = ws.where(wst.c.PatientId == patientid)
    ws = ws.cte('WaveSample')

    j = join(ws, ww, ws.c.WaveId == ww.c.Id)
    q = select(
        ws.c.TimeStamp,
        ws.c.PatientId,
        ww.c.Label,
        ws.c.WaveSamples,
        ww.c.SamplePeriod,
        ww.c.CAU,
        ww.c.CAL,
        ww.c.CSU,
        ww.c.CSL,
    ).select_from(j)
    return q


def build_meta():
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
    q = build_query(engine, dtbegin, dtend, patientid, labels)

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
    meta = build_meta()
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
