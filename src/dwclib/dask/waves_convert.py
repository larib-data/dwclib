import dask.dataframe as dd
import numpy as np
import pandas as pd
from dask.dataframe.utils import make_meta

from dwclib.waves.wave_unfold import wave_unfold


def unfold_row(row: pd.Series) -> pd.Series:
    basetime = 1000 * row.name[0].timestamp()
    msperiod = row['SamplePeriod']
    bytesamples = row['WaveSamples']
    calibs = [row[x] for x in ['CAU', 'CAL', 'CSU', 'CSL']]
    noscale = any([x is None for x in calibs])
    if noscale:
        realvals = wave_unfold(bytesamples, False, 0, 0, 0, 0)
    else:
        realvals = wave_unfold(bytesamples, True, *calibs)
    # Generate millisecond index
    timestamps = basetime + msperiod * np.arange(len(realvals))
    # Convert to datetime[64]
    timestamps = pd.to_datetime(timestamps, unit='ms', utc=True).to_numpy(
        dtype='datetime64[ns]'
    )
    return pd.Series(realvals, index=timestamps)


def unfold_pandas_dataframe(df, columns):
    idx = pd.MultiIndex.from_arrays(
        [df.index, df['Label']], names=('TimeStamp', 'Label')
    )
    df = df.set_index(idx)
    unfolded = df.apply(unfold_row, axis=1)
    transposed = unfolded.droplevel(0).groupby(level=0).max().T
    missing_columns = set(columns) - set(transposed.columns)
    result = transposed.assign(**{c: np.nan for c in missing_columns})
    return result.reindex(sorted(result.columns), axis=1)


def convert_dataframe(ddf: dd.DataFrame, labels=None, npartitions=None) -> dd.DataFrame:
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
