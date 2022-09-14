from datetime import datetime
from typing import List, Optional, Union

import numpy as np
import pandas as pd
from dwclib.common.db import dwcuri
from dwclib.common.meta import numerics_meta_tz
from dwclib.common.numerics import run_numerics_query
from pandas.api.types import is_list_like


def read_numerics(
    patientids: Union[None, str, List[str]],
    dtbegin: Union[str, datetime],
    dtend: Union[str, datetime],
    labels: Optional[List[str]] = None,
    sublabels: Optional[List[str]] = None,
    pivot: bool = True,
    uri: Optional[str] = None,
) -> pd.DataFrame:
    if not uri:
        uri = dwcuri
    if labels is None:
        labels = []
    if sublabels is None:
        sublabels = []
    if is_list_like(patientids) and len(patientids) == 1:
        patientids = patientids[0]
    df = run_numerics_query(uri, dtbegin, dtend, patientids, labels, sublabels)
    if pivot:
        df = pivot_numerics(df)
    single_patient = patientids is not None and not is_list_like(patientids)
    if single_patient:
        df.columns = df.columns.droplevel(0)
    # Other way to check if there is a single patientid:
    # if len(df.columns.get_level_values(0).drop_duplicates()) == 1:
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
