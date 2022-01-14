from datetime import timedelta

from ..db.engines import dwcuri
from .waves_convert import convert_dataframe
from .waves_sql import read_wave_chunks


def read_waves(
    patientid,
    dtbegin,
    dtend,
    labels=[],
    interval=timedelta(hours=1),
    npartitions=None,
    uri=None,
):
    if not uri:
        uri = dwcuri
    ddf = read_wave_chunks(patientid, dtbegin, dtend, uri, labels, interval)
    return convert_dataframe(ddf, labels, npartitions)
