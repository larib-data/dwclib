from datetime import timedelta
from itertools import count

from dwclib.common.meta import waves_meta
from dwclib.common.waves import run_waves_query

import dask.dataframe as dd
from dask import delayed


def build_divisions(dtbegin, dtend, interval):
    ranges = []
    for i in count():
        beg = dtbegin + i * interval
        end = beg + interval
        ranges.append((beg, end))
        if end >= dtend:
            break
    divisions = [beg for beg, _ in ranges]
    divisions.append(dtend)
    return (ranges, divisions)


def read_wave_chunks(
    patientid,
    dtbegin,
    dtend,
    uri,
    labels=[],
    interval=timedelta(hours=1),
):
    ranges, divisions = build_divisions(dtbegin, dtend, interval)
    parts = []
    for begin, end in ranges:
        parts.append(
            delayed(run_waves_query)(
                uri,
                begin,
                end,
                patientid,
                labels,
                True,
            )
        )
    return dd.from_delayed(parts, waves_meta, divisions=divisions)
