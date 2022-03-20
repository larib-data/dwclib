from collections import defaultdict
from datetime import datetime
from multiprocessing.pool import ThreadPool
from typing import List, Optional, Union

import pandas as pd
from dwclib.common.db import dwcuri
from dwclib.common.wave_unfold import unfold_row
from dwclib.common.waves import run_waves_query


def read_waves(
    patientid: str,
    dtbegin: Union[str, datetime],
    dtend: Union[str, datetime],
    labels: List[str] = [],
    uri: str = None,
) -> Optional[pd.DataFrame]:
    if not uri:
        uri = dwcuri
    df = run_waves_query(uri, dtbegin, dtend, patientid, labels)
    return unfold_pandas_dataframe(df)


def unfold_pandas_dataframe(df: pd.DataFrame) -> Optional[pd.DataFrame]:
    databuffer = defaultdict(list)
    for row in df:
        srow = unfold_row(row)
        databuffer[row['Label']].append(srow)
    if not databuffer:
        return None
    concatter = dictconcatter(databuffer)
    with ThreadPool() as pool:
        pool.map(concatter, databuffer.keys())
    return pd.DataFrame(databuffer)


def dictconcatter(d) -> pd.DataFrame:
    def dictconcat_runner(k):
        d[k] = pd.concat(d[k], axis=0, copy=False).groupby(level=0).max()

    return dictconcat_runner
