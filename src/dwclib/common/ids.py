from typing import Iterable, cast

from pandas.api.types import is_list_like

PatientIds = None | str | list[str]


def squeeze_ids(ids: PatientIds) -> PatientIds:
    """Unwrap a one-element list of patient ids into a single id."""
    if is_list_like(ids):
        ids = list(cast(Iterable[str], ids))
        if len(ids) == 1:
            return ids[0]
    return ids
