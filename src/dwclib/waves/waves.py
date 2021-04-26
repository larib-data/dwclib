from typing import Optional
from collections import defaultdict
from multiprocessing.pool import ThreadPool

import numpy as np
import pandas as pd
from sqlalchemy import select, func, column

from dwclib.waves.wave_unfold import wave_unfold


def unfold_row(basetime, bytesamples, period, cau, cal, csu, csl) -> pd.Series:
    calibs = [cau, cal, csu, csl]
    doscale = not any([x is None for x in calibs])
    cau, cal, csu, csl = [float(x) if x is not None else 0 for x in calibs]
    realvals = wave_unfold(bytesamples, doscale, cau, cal, csu, csl)
    # Generate millisecond index
    timestamps = basetime + period * np.arange(len(realvals))
    # Convert to datetime[64]
    timestamps = timestamps.astype('M8[ms]')
    return pd.Series(realvals, index=timestamps)


def dictconcatter(d) -> pd.DataFrame:
    def dictconcat_runner(k):
        d[k] = pd.concat(d[k], axis=0, copy=False).groupby(level=0).max()

    return dictconcat_runner


def get_wave_data(conn, dtbegin, dtend, patientid=None) -> Optional[pd.DataFrame]:
    q = build_wave_query(dtbegin, dtend, patientid)
    return run_wave_query(conn, q)


def build_wave_query(dtbegin, dtend, patientid=None):
    columns = [
        'DateTime',
        'PatientId',
        'Label',
        'Samples',
        'PeriodMs',
        'CAU',
        'CAL',
        'CSU',
        'CSL',
    ]
    # Leave out 'UnitLabel'
    q = select([column(c) for c in columns])
    q = q.select_from(func.LrbWaveChunksForPeriod(dtbegin, dtend))
    if patientid:
        q = q.where(column('PatientId') == patientid)
    return q


def run_wave_query(conn, q) -> Optional[pd.DataFrame]:
    res = conn.execute(q)
    databuffer = defaultdict(list)
    for row in res:
        basetime = 1000 * row['DateTime'].timestamp()
        srow = unfold_row(
            basetime,
            row['Samples'],
            int(row['PeriodMs']),
            row['CAU'],
            row['CAL'],
            row['CSU'],
            row['CSL'],
        )
        databuffer[row['Label']].append(srow)

    if not databuffer:
        return None

    concatter = dictconcatter(databuffer)
    with ThreadPool() as pool:
        pool.map(concatter, databuffer.keys())

    df = pd.DataFrame(databuffer)
    return df
