import pandas as pd
from dwclib.common.meta import waves_meta
from sqlalchemy import MetaData, Table, create_engine, join, select


def run_waves_query(uri, dtbegin, dtend, patientid, labels, naive_datetime=False):
    engine = create_engine(uri)
    q = build_waves_query(engine, dtbegin, dtend, patientid, labels)

    with engine.connect() as conn:
        df = pd.read_sql(q, conn, index_col='TimeStamp')
    engine.dispose()
    if len(df) == 0:
        return waves_meta
    else:
        dtidx = pd.to_datetime(df.index, utc=True)
        df.index = dtidx.to_numpy(dtype='datetime64[ns]') if naive_datetime else dtidx
        return df.astype(waves_meta.dtypes.to_dict(), copy=False)


def build_waves_query(engine, dtbegin, dtend, patientid, labels):
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
    ww = ww.with_hint(wwt, 'WITH (NOLOCK)')
    ww = ww.where(wwt.c.TimeStamp >= dtbegin)
    ww = ww.where(wwt.c.TimeStamp < dtend)
    if labels:
        ww = ww.where(wwt.c.Label.in_(labels))
    ww = ww.cte('Wave')

    ws = select(wst.c.TimeStamp, wst.c.WaveId, wst.c.WaveSamples, wst.c.PatientId)
    ws = ws.with_hint(wst, 'WITH (NOLOCK)')
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
