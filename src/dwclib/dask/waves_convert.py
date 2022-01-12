import dask.dataframe as dd
import numpy as np
import pandas as pd
import xarray as xr

from dwclib.waves.wave_unfold import wave_unfold


def unfold_row(row: pd.Series) -> xr.DataArray:
    basetime = 1000 * row.name.timestamp()
    lbl = row['Label']
    bytesamples = row['WaveSamples']
    period = row['SamplePeriod']
    cau = row['CAU']
    cal = row['CAL']
    csu = row['CSU']
    csl = row['CSL']

    calibs = [cau, cal, csu, csl]
    doscale = not any([x is None for x in calibs])
    if doscale:
        cau, cal, csu, csl = (float(x) for x in calibs)
    else:
        cau, cal, csu, csl = (0, 0, 0, 0)
    realvals = wave_unfold(bytesamples, doscale, cau, cal, csu, csl)
    # Generate millisecond index
    timestamps = basetime + period * np.arange(len(realvals))
    # Convert to datetime[64]
    timestamps = timestamps.astype('datetime64[ms]')
    return xr.DataArray(realvals, coords=[timestamps], dims=['datetime'], name=lbl)


def dataframe_to_dataset(ddf: dd.DataFrame) -> xr.Dataset:
    arrays = ddf.map_partitions(
        lambda df: df.apply(unfold_row, axis=1), meta=('x', object)
    )
    ds = xr.merge(arrays, compat='override', join='outer')
    return ds
