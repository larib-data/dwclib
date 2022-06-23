from typing import List, Optional

from dwclib.common.meta import numerics_meta
from dwclib.common.numerics import run_numerics_query
from dwclib.dask.generic import read_data


def read_numerics(
    patientids,
    dtbegin,
    dtend,
    labels: Optional[List[str]] = None,
    interval=None,
    uri=None,
):
    if labels is None:
        labels = []
    return read_data(
        runner=run_numerics_query,
        meta=numerics_meta,
        dtbegin=dtbegin,
        dtend=dtend,
        uri=uri,
        interval=interval,
        patientids=patientids,
        labels=labels,
        naive_datetime=True,
    )
