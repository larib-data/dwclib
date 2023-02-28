from datetime import datetime
from functools import lru_cache
from importlib import resources
from typing import List, Optional, Union

import pandas as pd
from pandas.api.types import is_list_like
from sqlalchemy import MetaData, Table, create_engine, func, select

from dwclib import assets
from dwclib.common.db import dwcuri


def read_alerts(
    patientids: Union[None, str, List[str]],
    dtbegin: Union[str, datetime],
    dtend: Union[str, datetime],
    uri: Optional[str] = None,
) -> pd.DataFrame:
    if not uri:
        uri = dwcuri
    if is_list_like(patientids) and len(patientids) == 1:
        patientids = patientids[0]
    df = run_alerts_query(uri, dtbegin, dtend, patientids)
    return df


def run_alerts_query(
    uri: str,
    dtbegin: Union[str, datetime],
    dtend: Union[str, datetime],
    patientids: Union[None, str, List[str]],
) -> pd.DataFrame:
    engine = create_engine(uri)
    q = build_alerts_query(engine, dtbegin, dtend, patientids)
    # print(q.compile(engine))

    with engine.connect() as conn:
        df = pd.read_sql(q, conn, index_col='AlertId')
    engine.dispose()

    rosettadf = load_rosetta_data()
    rosettadf = rosettadf[['DisplayName', 'Description', 'Group']]
    dfr = df.join(rosettadf, on='Source')
    return dfr.drop(columns=['Source'])


def build_alerts_query(engine, dtbegin, dtend, patientids):
    dbmeta = MetaData(bind=engine)
    at = Table('Alert_', dbmeta, schema='_Export', autoload=True, autoload_with=engine)

    aq = select(
        at.c.AlertId,
        func.min(at.c.TimeStamp).label('Begin'),
        func.max(at.c.TimeStamp).label('End'),
        func.max(at.c.Source).label('Source'),
        func.max(at.c.Label).label('Label'),
    )
    aq = aq.with_hint(at, 'WITH (NOLOCK)')
    aq = aq.where(at.c.TimeStamp >= dtbegin)
    aq = aq.where(at.c.TimeStamp < dtend)
    aq = aq.group_by(at.c.AlertId)

    if patientids:
        if is_list_like(patientids):
            aq = aq.where(at.c.PatientId.in_(patientids))
        else:
            aq = aq.where(at.c.PatientId == patientids)
    return aq


@lru_cache
def load_rosetta_data():
    cols = [
        'Group',
        'REFID',
        'DisplayName',
        'Description',
        'Vendor_VMD',
        'Vendor_UOM',
        'UOM_MDC',
        'UOM_UCUM',
        'Vendor_ID',
        'CF_CODE10',
    ]
    dtypes = {c: 'string' for c in cols}
    dtypes['CF_CODE10'] = 'Int64'

    with resources.open_text(assets, 'rosetta_terms.csv') as fd:
        df = pd.read_csv(fd, usecols=cols, index_col='CF_CODE10', dtype=dtypes)
    df = df[df['Vendor_ID'] == 'Philips']
    df = df.drop(columns=['Vendor_ID'])
    df = df.loc[df.index.dropna()]
    df = df.loc[~df.index.duplicated()]
    return df
