from collections import defaultdict
from multiprocessing.pool import ThreadPool
from typing import Optional

import numpy as np
import pandas as pd
from sqlalchemy import column
from sqlalchemy import create_engine
from sqlalchemy import func
from sqlalchemy import select

from dwclib.db.engines import dwcuri
from dwclib.waves.wave_unfold import wave_unfold


def read_waves(
    patientid,
    dtbegin,
    dtend,
    labels=[],
    uri=None,
) -> Optional[pd.DataFrame]:
    q = build_wave_query(dtbegin, dtend, patientid)
    if labels:
        q = q.where(column('Label').in_(labels))
    if not uri:
        uri = dwcuri
    engine = create_engine(uri)
    with engine.connect() as conn:
        df = run_wave_query(conn, q)
    return df


def build_wave_query(dtbegin, dtend, patientid):
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


def unfold_row(basetime, bytesamples, period, cau, cal, csu, csl) -> pd.Series:
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
    timestamps = pd.to_datetime(timestamps, unit='ms', utc=True)
    return pd.Series(realvals, index=timestamps)


def dictconcatter(d) -> pd.DataFrame:
    def dictconcat_runner(k):
        d[k] = pd.concat(d[k], axis=0, copy=False).groupby(level=0).max()

    return dictconcat_runner
