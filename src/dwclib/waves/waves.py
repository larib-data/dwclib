from collections import defaultdict
from datetime import datetime
from multiprocessing.pool import ThreadPool
from typing import List, Optional, Union

import pandas as pd
from dwclib.common.db import dwcuri
from dwclib.common.meta import waves_meta_tz
from dwclib.common.wave_unfold import unfold_row
from dwclib.common.waves import run_waves_query


def read_waves(
    patientid: str,
    dtbegin: Union[str, datetime],
    dtend: Union[str, datetime],
    labels: Optional[List[str]] = None,
    uri: str = None,
) -> pd.DataFrame:
    if not uri:
        uri = dwcuri
    if labels is None:
        labels = []
    df = run_waves_query(uri, dtbegin, dtend, patientid, labels)
    return unfold_pandas_dataframe(df)


def unfold_pandas_dataframe(df: pd.DataFrame) -> pd.DataFrame:
    databuffer = defaultdict(list)
    for _, row in df.iterrows():
        srow = unfold_row(row, naive_datetime=False)
        databuffer[row['Label']].append(srow)
    if not databuffer:
        return waves_meta_tz
    concatter = dictconcatter(databuffer)
    with ThreadPool() as pool:
        pool.map(concatter, databuffer.keys())
    return pd.DataFrame(databuffer)


def dictconcatter(d) -> pd.DataFrame:
    def dictconcat_runner(k):
        d[k] = pd.concat(d[k], axis=0, copy=False).groupby(level=0).max()

    return dictconcat_runner
