from datetime import timedelta
from typing import List, Optional

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
    if labels is None:
        labels = []
    ddf = read_data(
        run_waves_query,
        waves_meta,
        dtbegin,
        dtend,
        uri,
        interval,
        patientid=patientid,
        labels=labels,
        naive_datetime=True,
    )
    return convert_dataframe(ddf, labels, npartitions)
