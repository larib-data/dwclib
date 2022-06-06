from datetime import timedelta
from typing import List, Optional

from dwclib.common.db import dwcuri

from .numerics_sql import read_numerics as read_numerics_sql

one_hour = timedelta(hours=1)


def read_numerics(
    patientids,
    dtbegin,
    dtend,
    labels: Optional[List[str]] = None,
    interval=one_hour,
    uri=None,
):
    if not uri:
        uri = dwcuri
    if labels is None:
        labels = []
    return read_numerics_sql(
        patientids,
        dtbegin,
        dtend,
        uri,
        labels,
        interval,
    )
