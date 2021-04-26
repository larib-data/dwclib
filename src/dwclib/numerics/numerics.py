from sqlalchemy import select, func, column

import numpy as np
import pandas as pd


def get_numerics_data(conn, dtbegin, dtend, patientid=None) -> Optional[pd.DataFrame]:
    q = build_numerics_query(dtbegin, dtend, patientid)
    return run_numerics_query(conn, q)


def build_numerics_query(dtbegin, dtend, patientid=None):
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
    df = pd.read_sql(q, conn, params={})
    # Remove NaN rows
    df = df.dropna(axis=0, how='any', subset=['Value'])
    if not len(df.index):
        return None

    df['Value'] = df['Value'].astype('float32')

    df = df.pivot_table(
        index='DateTime', columns='Label', values='Value', aggfunc=np.nanmax
    )
    return df
