from typing import Optional

import numpy as np
import pandas as pd
from sqlalchemy import column
from sqlalchemy import func
from sqlalchemy import select


def get_numerics_data(
    conn, dtbegin, dtend, patientid, labels=[]
) -> Optional[pd.DataFrame]:
    q = build_numerics_query(dtbegin, dtend, patientid)
    if labels:
        q = q.where(column('Label').in_(labels))
    df = run_numerics_query(conn, q)
    if len(df.columns.get_level_values(0).drop_duplicates()) == 1:
        # Only 1 patient
        df.columns = df.columns.droplevel(0)
    return df


def build_numerics_query(dtbegin, dtend, patientid):
    columns = [
        'DateTime',
        'PatientId',
        'Label',
        'Value',
    ]
    q = select([column(c) for c in columns])
    q = q.select_from(func.LrbNumericsForPeriod(dtbegin, dtend))
    if patientid:
        q = q.where(column('PatientId') == patientid)
    return q


def run_numerics_query(conn, q) -> Optional[pd.DataFrame]:
    df = pd.read_sql(q, conn)
    df = df.dropna(axis=0, how='any', subset=['Value'])
    if not len(df.index):
        return df
    df['Value'] = df['Value'].astype('float32')
    df = df.pivot_table(
        index='DateTime',
        columns=['PatientId', 'Label'],
        values='Value',
        aggfunc=np.nanmax,
    )
    df.index = df.index.astype('datetime64[ns]')
    return df
