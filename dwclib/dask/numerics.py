from datetime import timedelta
from typing import List, Optional

from dwclib.common.meta import numerics_meta
from dwclib.common.numerics import run_numerics_query
from dwclib.dask.generic import read_data

one_hour = timedelta(hours=1)


def read_numerics(
    patientids,
    dtbegin,
    dtend,
    labels: Optional[List[str]] = None,
    interval=one_hour,
    uri=None,
):
    if labels is None:
        labels = []
    return read_data(
        run_numerics_query,
        numerics_meta,
        dtbegin,
        dtend,
        uri,
        interval,
        patientids=patientids,
        labels=labels,
        naive_datetime=True,
    )
