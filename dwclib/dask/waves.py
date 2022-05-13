from datetime import timedelta

from dwclib.common.db import dwcuri

from .waves_convert import convert_dataframe
from .waves_sql import read_wave_chunks

one_hour = timedelta(hours=1)


def read_waves(
    patientid,
    dtbegin,
    dtend,
    labels=[],
    interval=one_hour,
    npartitions=None,
    uri=None,
):
    if not uri:
        uri = dwcuri
    ddf = read_wave_chunks(patientid, dtbegin, dtend, uri, labels, interval)
    return convert_dataframe(ddf, labels, npartitions)
