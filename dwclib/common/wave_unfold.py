import numpy as np
import pandas as pd
from numba import njit


def unfold_row(row: pd.Series, naive_datetime=True) -> pd.Series:
    # XXX unify following line once we use the same unfold_row function for pandas and dask
    begintime = row.name[0] if isinstance(row.name, tuple) else row.name
    basetime = 1000 * begintime.timestamp()
    msperiod = row['SamplePeriod']
    bytesamples = row['WaveSamples']
    calibs = [row[x] for x in ['CAU', 'CAL', 'CSU', 'CSL']]
    noscale = any([x is None for x in calibs])
    if noscale:
        realvals = wave_unfold(bytesamples, 0, 0, 0, 0)
    else:
        realvals = wave_unfold(bytesamples, *calibs)
    # Generate millisecond index
    timestamps = basetime + msperiod * np.arange(len(realvals))
    # Convert to datetime[64]
    timestamps = pd.to_datetime(timestamps, unit='ms', utc=True)
    if naive_datetime:
        timestamps = timestamps.to_numpy(dtype='datetime64[ns]')
    return pd.Series(realvals, index=timestamps)


@njit
def wave_unfold(indata: bytes, cau: float, cal: float, csu: int, csl: int):
    if len(indata) % 2:
        indata = indata[:-1]
    int16le = np.dtype('<i2')
    npindata = np.frombuffer(indata, dtype=int16le)
    if (csu - csl) != 0:
        m = (cau - cal) / (csu - csl)
        b = cau - m * csu
    else:
        m = 1
        b = 0
    outdata = m * npindata.astype(np.float32) + b
    return outdata
