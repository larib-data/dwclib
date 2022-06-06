from datetime import timedelta
from typing import List, Optional

from dwclib.common.db import dwcuri
from dwclib.common.meta import waves_meta
from dwclib.common.waves import run_waves_query
from dwclib.dask.generic import read_data

from .waves_convert import convert_dataframe

one_hour = timedelta(hours=1)


def read_waves(
    patientid,
    dtbegin,
    dtend,
    labels: Optional[List[str]] = None,
    interval=one_hour,
    npartitions=None,
    uri=None,
):
    if not uri:
        uri = dwcuri
    if labels is None:
        labels = []
    ddf = read_data(
        run_waves_query, waves_meta, patientid, dtbegin, dtend, uri, labels, interval
    )
    return convert_dataframe(ddf, labels, npartitions)
