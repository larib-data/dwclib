from typing import List

import pandas as pd
from sqlalchemy import asc
from sqlalchemy import cast
from sqlalchemy import create_engine
from sqlalchemy import func
from sqlalchemy import or_
from sqlalchemy import select
from sqlalchemy.types import Text

from dwclib.db import engines


def read_patients(
    patientid: str = None,
    name: str = None,
    ipp: str = None,
    dtbegin: str = None,
    dtend: str = None,
    clinicalunit: str = None,
    bedlabel: str = None,
    wavelabels: List[str] = [],
    numericlabels: List[str] = [],
    uri=None,
):
    q = build_query(
        patientid,
        name,
        ipp,
        dtbegin,
        dtend,
        clinicalunit,
        bedlabel,
        wavelabels,
        numericlabels,
    )
    if not uri:
        uri = engines.pguri
    engine = create_engine(uri)
    with engine.connect() as conn:
        df = pd.read_sql(q, conn, index_col='patientid')
    return df


def build_query(
    patientid: str = None,
    name: str = None,
    ipp: str = None,
    dtbegin: str = None,
    dtend: str = None,
    clinicalunit: str = None,
    bedlabel: str = None,
    wavelabels: List[str] = [],
    numericlabels: List[str] = [],
):
    t_patients = engines.patients_t
    p = t_patients.c
    t_patientlabels = engines.patientlabelst
    pl = t_patientlabels.c

    if name:
        # Build a subquery to allow approximate name search
        subq = select(
            [
                t_patients,
                func.levenshtein(func.lower(p.lastname), func.lower(name)).label(
                    'distance'
                ),
            ]
        ).cte('subq')
        t_patients = subq
        p = t_patients.c

    q = select([t_patients, pl.numericlabels, pl.wavelabels])
    q = q.select_from(
        t_patients.join(
            t_patientlabels,
            cast(pl.patientid, Text) == p.patientid,
            isouter=True,
        )
    )
    if patientid:
        q = q.where(p.patientid == patientid)
    if name:
        q = q.where(p.distance < 3)
        q = q.order_by(asc(p.distance))
    if ipp:
        q = q.where(p.lifetimeid == ipp)
    if bedlabel:
        q = q.where(p.bedlabel == bedlabel)
    if clinicalunit:
        q = q.where(p.clinicalunit == clinicalunit)
    if dtbegin:
        q = q.where(or_(p.data_begin >= dtbegin, p.data_end >= dtbegin))
    if dtend:
        q = q.where(or_(p.data_begin <= dtend, p.data_end <= dtend))
    if numericlabels:
        q = q.where(pl.numericlabels.contains(numericlabels))
    if wavelabels:
        q = q.where(pl.wavelabels.contains(wavelabels))
    return q
