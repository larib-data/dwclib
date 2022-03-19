from datetime import datetime
from typing import List, Optional, Union

import numpy as np
import pandas as pd
from dwclib.db.engines import dwcuri
from dwclib.db.numerics_query import build_numerics_query
from pandas.api.types import is_list_like
from sqlalchemy import column, create_engine


def read_numerics(
    patientids: Union[str, List[str]],
    dtbegin: Union[str, datetime],
    dtend: Union[str, datetime],
    labels: List[str] = [],
    uri: Optional[str] = None,
) -> pd.DataFrame:
    if not uri:
        uri = dwcuri
    engine = create_engine(uri)
    q = build_numerics_query(engine, dtbegin, dtend, patientids, labels)
    with engine.connect() as conn:
        df = run_numerics_query(conn, q)
    # if len(df.columns.get_level_values(0).drop_duplicates()) == 1:
    if not is_list_like(patientids):
        # Only 1 patient
        df.columns = df.columns.droplevel(0)
    return df


def run_numerics_query(conn, q) -> Optional[pd.DataFrame]:
    df = pd.read_sql(q, conn)
    df = df.dropna(axis=0, how='any', subset=['Value'])
    if not len(df.index):
        return df
    df['Value'] = df['Value'].astype('float32')
    df = df.pivot_table(
        index=df.index,
        columns=['PatientId', 'SubLabel'],
        values='Value',
        aggfunc=np.nanmax,
    )
    df.index = pd.to_datetime(df.index, utc=True)
    return df
