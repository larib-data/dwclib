import numpy as np
import pandas as pd
from numba import njit


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


@njit
def wave_unfold(
    indata: bytes, doscale: bool, cau: float, cal: float, csu: int, csl: int
):
    npindata = np.frombuffer(indata, dtype=np.int16)
    if doscale:
        m = (cau - cal) / (csu - csl)
        b = cau - m * csu
    else:
        m = 1
        b = 0
    outdata = m * npindata.astype(np.float32) + b
    return outdata
