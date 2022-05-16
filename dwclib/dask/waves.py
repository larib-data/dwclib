from datetime import timedelta
from typing import List, Optional

from dwclib.common.db import dwcuri

from .waves_convert import convert_dataframe
from .waves_sql import read_wave_chunks

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
    ddf = read_wave_chunks(patientid, dtbegin, dtend, uri, labels, interval)
    return convert_dataframe(ddf, labels, npartitions)
