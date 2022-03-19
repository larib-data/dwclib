from collections import defaultdict
from datetime import datetime
from multiprocessing.pool import ThreadPool
from typing import List, Optional, Union

import numpy as np
import pandas as pd
from dwclib.common.engines import dwcuri
from dwclib.common.waves_query import build_waves_query
from dwclib.waves.wave_unfold import wave_unfold
from sqlalchemy import column, create_engine


def read_waves(
    patientid: str,
    dtbegin: Union[str, datetime],
    dtend: Union[str, datetime],
    labels: List[str] = [],
    uri: str = None,
) -> Optional[pd.DataFrame]:
    if not uri:
        uri = dwcuri
    engine = create_engine(uri)
    q = build_waves_query(engine, dtbegin, dtend, patientid, labels)
    with engine.connect() as conn:
        df = run_wave_query(conn, q)
    return df


def run_wave_query(conn, q) -> Optional[pd.DataFrame]:
    res = conn.execute(q)
    databuffer = defaultdict(list)
    for row in res:
        basetime = 1000 * row['TimeStamp'].timestamp()
        srow = unfold_row(
            basetime,
            row['WaveSamples'],
            int(row['SamplePeriod']),
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
