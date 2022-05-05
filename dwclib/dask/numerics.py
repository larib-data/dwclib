from datetime import timedelta

from dwclib.common.db import dwcuri

from .numerics_dask import read_numerics as read_numerics_dask
from .numerics_sql import read_numerics as read_numerics_sql


def read_numerics(
    patientids,
    dtbegin,
    dtend,
    labels=[],
    interval=timedelta(hours=1),
    joinengine='sql',
    uri=None,
):
    if not uri:
        uri = dwcuri
    if joinengine == 'sql':
        return read_numerics_sql(
            patientids,
            dtbegin,
            dtend,
            uri,
            labels,
            interval,
        )
    elif joinengine == 'dask':
        return read_numerics_dask(
            patientids,
            dtbegin,
            dtend,
            uri,
            labels,
            interval,
        )
    else:
        return None
