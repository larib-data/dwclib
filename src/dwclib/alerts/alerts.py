from datetime import datetime
from functools import lru_cache
from importlib import resources
from typing import List, Optional, Union

import pandas as pd
from pandas.api.types import is_list_like
from sqlalchemy import MetaData, Table, create_engine, func, select

from dwclib import assets
from dwclib.common.db import dwcuri


def read_alerts(
    patientids: Union[None, str, List[str]],
    dtbegin: Union[str, datetime],
    dtend: Union[str, datetime],
    uri: Optional[str] = None,
) -> pd.DataFrame:
    if not uri:
        uri = dwcuri
    if is_list_like(patientids) and len(patientids) == 1:
        patientids = patientids[0]
    df = run_alerts_query(uri, dtbegin, dtend, patientids)
    return df


def run_alerts_query(
    uri: str,
    dtbegin: Union[str, datetime],
    dtend: Union[str, datetime],
    patientids: Union[None, str, List[str]],
) -> pd.DataFrame:
    engine = create_engine(uri)
    q = build_alerts_query(engine, dtbegin, dtend, patientids)

    with engine.connect() as conn:
        df = pd.read_sql(q, conn, index_col="begin")
    engine.dispose()

    metadf = load_alerts_meta()
    metadf = metadf[["source_label", "mdc_alert", "alert_kind", "severity"]]
    dfr = df.join(metadf, on=["Source", "Code"])
    return dfr.drop(columns=["Source", "Code"])


def build_alerts_query(engine, dtbegin, dtend, patientids):
    dbmeta = MetaData(schema="_Export")
    at = Table("Alert_", dbmeta, autoload_with=engine)

    aq = select(
        func.min(at.c.TimeStamp).label("begin"),
        func.max(at.c.TimeStamp).label("end"),
        func.max(at.c.Source).label("Source"),
        func.max(at.c.Code).label("Code"),
        func.max(at.c.Label).label("alert_label"),
    )
    aq = aq.with_hint(at, "WITH (NOLOCK)")
    aq = aq.where(at.c.TimeStamp >= dtbegin)
    aq = aq.where(at.c.TimeStamp < dtend)
    aq = aq.group_by(at.c.AlertId)

    if patientids:
        if is_list_like(patientids):
            aq = aq.where(at.c.PatientId.in_(patientids))
        else:
            aq = aq.where(at.c.PatientId == patientids)
    return aq


@lru_cache
def load_alerts_meta():
    cols = [
        "physioid",
        "alert_code",
        "mdc_alert",
        "alert_kind",
        "severity",
        "source_label",
    ]
    dtypes = {c: "string" for c in cols}
    dtypes["physioid"] = "Int64"
    dtypes["alert_code"] = "Int64"

    with resources.open_text(assets, "alert_ref.csv") as fd:
        df = pd.read_csv(
            fd, usecols=cols, index_col=["physioid", "alert_code"], dtype=dtypes
        )
    return df
