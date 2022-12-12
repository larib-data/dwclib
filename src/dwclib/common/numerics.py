from datetime import datetime
from typing import List, Union

import pandas as pd
from dwclib.common.meta import numerics_meta, numerics_meta_tz
from pandas.api.types import is_list_like
from sqlalchemy import MetaData, Table, create_engine, join, select


def run_numerics_query(
    uri: str,
    dtbegin: Union[str, datetime],
    dtend: Union[str, datetime],
    patientids: Union[None, str, List[str]],
    labels: List[str],
    sublabels: List[str],
    naive_datetime: bool = False,
) -> pd.DataFrame:
    engine = create_engine(uri)
    q = build_numerics_query(engine, dtbegin, dtend, patientids, labels, sublabels)

    with engine.connect() as conn:
        df = pd.read_sql(q, conn, index_col='TimeStamp')
    engine.dispose()
    if len(df) == 0:
        return numerics_meta if naive_datetime else numerics_meta_tz
    else:
        dtidx = pd.to_datetime(df.index, utc=True)
        df.index = dtidx.to_numpy(dtype='datetime64[ns]') if naive_datetime else dtidx
        return df.astype(numerics_meta.dtypes.to_dict(), copy=False)


def build_numerics_query(engine, dtbegin, dtend, patientids, labels, sublabels):
    dbmeta = MetaData(bind=engine)
    nnt = Table(
        'Numeric_', dbmeta, schema='_Export', autoload=True, autoload_with=engine
    )
    nvt = Table(
        'NumericValue_', dbmeta, schema='_Export', autoload=True, autoload_with=engine
    )

    nn = select(nnt.c.TimeStamp, nnt.c.Id, nnt.c.Label, nnt.c.SubLabel)
    nn = nn.with_hint(nnt, 'WITH (NOLOCK)')
    nn = nn.where(nnt.c.TimeStamp >= dtbegin)
    nn = nn.where(nnt.c.TimeStamp < dtend)
    if labels:
        nn = nn.where(nnt.c.Label.in_(labels))
    if sublabels:
        nn = nn.where(nnt.c.SubLabel.in_(sublabels))
    nn = nn.cte('Numeric')

    nv = select(nvt.c.TimeStamp, nvt.c.NumericId, nvt.c.Value, nvt.c.PatientId)
    nv = nv.with_hint(nvt, 'WITH (NOLOCK)')
    nv = nv.where(nvt.c.TimeStamp >= dtbegin)
    nv = nv.where(nvt.c.TimeStamp < dtend)
    nv = nv.where(nvt.c.Value is not None)
    if patientids:
        if is_list_like(patientids):
            nv = nv.where(nvt.c.PatientId.in_(patientids))
        else:
            nv = nv.where(nvt.c.PatientId == patientids)
    nv = nv.cte('NumericValue')

    j = join(nv, nn, nv.c.NumericId == nn.c.Id)
    q = select(
        nv.c.TimeStamp, nv.c.PatientId, nn.c.Label, nn.c.SubLabel, nv.c.Value
    ).select_from(j)
    return q
