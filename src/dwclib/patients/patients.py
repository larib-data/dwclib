from typing import List

import pandas as pd
from sqlalchemy import MetaData
from sqlalchemy import Table
from sqlalchemy import asc
from sqlalchemy import create_engine
from sqlalchemy import func
from sqlalchemy import or_
from sqlalchemy import select

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
    if not uri:
        uri = engines.pguri
    q = build_query(
        uri,
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
    engine = create_engine(uri)
    with engine.connect() as conn:
        df = pd.read_sql(q, conn, index_col='patientid')
    return df


def build_query(
    uri,
    patientid,
    name,
    ipp,
    dtbegin,
    dtend,
    clinicalunit,
    bedlabel,
    wavelabels,
    numericlabels,
):
    pgdb = create_engine(uri)
    pgmeta = MetaData(bind=pgdb)

    t_patients = Table('patients', pgmeta, autoload_with=pgdb)
    p = t_patients.c
    t_patientlabels = Table('patientlabels', pgmeta, autoload_with=pgdb)
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
        t_patients.join(t_patientlabels, pl.patientid == p.patientid, isouter=True)
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
