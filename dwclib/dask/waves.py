from datetime import timedelta
from typing import List, Optional

from dwclib.common.meta import waves_meta
from dwclib.common.waves import run_waves_query
from dwclib.dask.generic import read_data

from .waves_convert import convert_dataframe


def read_waves(
    patientid,
    dtbegin,
    dtend,
    labels: Optional[List[str]] = None,
    interval=None,
    npartitions=None,
    uri=None,
):
    if labels is None:
        labels = []
    ddf = read_data(
        runner=run_waves_query,
        meta=waves_meta,
        dtbegin=dtbegin,
        dtend=dtend,
        uri=uri,
        interval=interval,
        patientid=patientid,
        labels=labels,
        naive_datetime=True,
    )
    return convert_dataframe(ddf, labels, npartitions)
