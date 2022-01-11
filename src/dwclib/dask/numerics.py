from datetime import timedelta

from .numerics_dask import read_numerics as read_numerics_dask
from .numerics_sql import read_numerics as read_numerics_sql


def read_numerics(
    patientids,
    dtbegin,
    dtend,
    uri,
    labels=[],
    interval=timedelta(hours=1),
    joinengine='sql',
):
    assert joinengine in ['sql', 'dask']
    if joinengine == 'sql':
        return read_numerics_sql(
            patientids,
            dtbegin,
            dtend,
            uri,
            labels,
            interval,
        )
    else:
        return read_numerics_dask(
            patientids,
            dtbegin,
            dtend,
            uri,
            labels,
            interval,
        )
