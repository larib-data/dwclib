from datetime import datetime
from typing import List, Optional, Union

import numpy as np
import pandas as pd
from dwclib.common.db import dwcuri
from dwclib.common.meta import numerics_meta_tz
from dwclib.common.numerics import run_numerics_query
from pandas.api.types import is_list_like


def read_numerics(
    patientids: Union[str, List[str]],
    dtbegin: Union[str, datetime],
    dtend: Union[str, datetime],
    labels: Optional[List[str]] = None,
    uri: Optional[str] = None,
) -> pd.DataFrame:
    if not uri:
        uri = dwcuri
    if labels is None:
        labels = []
    df = run_numerics_query(uri, dtbegin, dtend, patientids, labels)
    df = pivot_numerics(df)
    # if len(df.columns.get_level_values(0).drop_duplicates()) == 1:
    if not is_list_like(patientids):
        # Only 1 patient
        df.columns = df.columns.droplevel(0)
    return df


def pivot_numerics(df: pd.DataFrame) -> Optional[pd.DataFrame]:
    df = df.dropna(axis=0, how='any', subset=['Value'])
    if not len(df.index):
        numerics_meta_tz
    df = df.pivot_table(
        index=df.index,
        columns=['PatientId', 'SubLabel'],
        values='Value',
        aggfunc=np.nanmax,
    )
    return df
