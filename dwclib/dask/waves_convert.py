from typing import List, Optional

import numpy as np
import pandas as pd
from dwclib.common.wave_unfold import unfold_row

import dask.dataframe as dd
from dask.dataframe.utils import make_meta


def unfold_pandas_dataframe(df: pd.DataFrame, columns: List[str]) -> pd.DataFrame:
    idx = pd.MultiIndex.from_arrays(
        [df.index, df['Label']], names=('TimeStamp', 'Label')
    )
    df = df.set_index(idx)
    unfolded = df.apply(unfold_row, axis=1)
    transposed = unfolded.droplevel(0).groupby(level=0).max().T
    missing_columns = set(columns) - set(transposed.columns)
    result = transposed.assign(**{c: np.nan for c in missing_columns})
    return result.reindex(sorted(result.columns), axis=1)


def convert_dataframe(
    ddf: dd.DataFrame, labels: List[str] = [], npartitions: Optional[int] = None
) -> dd.DataFrame:
    if not labels:
        labels = ddf['Label'].unique().compute()
    labels = sorted(set(labels))
    dfmeta = pd.DataFrame(columns=labels, dtype='float32')
    idx = pd.DatetimeIndex([], name='TimeStamp')
    meta = make_meta(dfmeta, index=idx)
    if not npartitions:
        npartitions = 20 * ddf.npartitions
    ddf = ddf.repartition(npartitions)
    return ddf.map_partitions(unfold_pandas_dataframe, columns=labels, meta=meta)
