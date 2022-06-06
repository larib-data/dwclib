from datetime import timedelta
from itertools import count

import dask.dataframe as dd
from dask import delayed

one_hour = timedelta(hours=1)


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


def read_data(runner, meta, patientids, dtbegin, dtend, uri, labels, interval=one_hour):
    ranges, divisions = build_divisions(dtbegin, dtend, interval)
    parts = []
    for begin, end in ranges:
        parts.append(
            delayed(runner)(
                uri,
                begin,
                end,
                patientids,
                labels,
                True,
            )
        )
    return dd.from_delayed(parts, meta, divisions=divisions)