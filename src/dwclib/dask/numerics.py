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
    sublabels: Optional[List[str]] = None,
    interval: timedelta = one_hour,
    uri=None,
):
    if labels is None:
        labels = []
    if sublabels is None:
        sublabels = []
    return read_data(
        runner=run_numerics_query,
        meta=numerics_meta,
        dtbegin=dtbegin,
        dtend=dtend,
        uri=uri,
        interval=interval,
        patientids=patientids,
        labels=labels,
        sublabels=sublabels,
        naive_datetime=True,
    )
