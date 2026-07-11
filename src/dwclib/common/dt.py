from datetime import datetime
from typing import Optional, Union

import pandas as pd


def to_datetime(value: Union[None, str, datetime]) -> Optional[datetime]:
    """Validate and coerce a datetime-like value to a python datetime.

    Accepts ISO-8601 strings or datetime objects and returns a python
    ``datetime`` (or ``None``). Parsing here validates the input early and
    ensures the value is bound as a timestamp rather than a string.

    On the Postgres (psycopg3) backend this is required for correctness: the
    dialect derives the SQL bind cast from the python value's type, so a plain
    ``str`` compared against a ``timestamptz`` column is cast to ``::VARCHAR``
    and rejected. On SQL Server it is a validation/consistency convenience.
    """
    if value is None:
        return None
    return pd.to_datetime(value).to_pydatetime()
