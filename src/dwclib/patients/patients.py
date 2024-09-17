from typing import List, Optional
import pandas as pd
from sqlalchemy import MetaData, Table, asc, create_engine, func, or_, select
import sqlalchemy
from dwclib.common.db import dwcuri, pguri


def read_patient(*args, **kwargs) -> Optional[pd.Series]:
    """Reads a single patient from DWCmeta database.

    Retrieves a pandas series consisting of the patient which matches the search criteria.

    Args:
        patientid: A DWC patient identifier
        name: A patient name
        firstname: A patient first name
        ipp: A patient lifetime identifier
        dtbegin: The beginning of available data samples
        dtend: The end of available data samples
        clinicalunit: The clinical unit the patient belongs to
        bedlabel: The bed / room in which the patient stays
        wavelabels: A list of waveform labels
        numericlabels: A list of numerics labels
        uri: Optional sqlalchemy URI for the database if not provided in the config file

    Returns:
        A pandas series corresponding to a patient stay.
    """  # noqa: DAR101,DAR102

    res = read_patients(*args, **kwargs, limit=1)
    if len(res):
        return res.iloc[0]


def read_patients(
    patientid: str = None,
    name: str = None,
    firstname: str = None,
    ipp: str = None,
    dtbegin: str = None,
    dtend: str = None,
    clinicalunit: str = None,
    bedlabel: str = None,
    wavelabels: Optional[List[str]] = None,
    numericlabels: Optional[List[str]] = None,
    numericsublabels: Optional[List[str]] = None,
    uri: Optional[str] = None,
    limit: Optional[int] = None,
) -> pd.DataFrame:
    """Reads patients from DWCmeta database.

    Retrieves a pandas dataframe consisting of individual patients which match the search criteria.

    Args:
        patientid: A DWC patient identifier
        name: A patient name
        firstname: A patient first name
        ipp: A patient lifetime identifier
        dtbegin: The beginning of available data samples
        dtend: The end of available data samples
        clinicalunit: The clinical unit the patient belongs to
        bedlabel: The bed / room in which the patient stays
        wavelabels: A list of waveform labels
        numericlabels: A list of numerics labels
        numericsublabels: A list of numerics sublabels
        uri: Optional sqlalchemy URI for the database if not provided in the config file
        limit: Maximum number of returned results

    Returns:
        A pandas dataframe with each row corresponding to an individual patient stay, indexed by unique DWC patient identifiers.
    """
    if not uri:
        uri = pguri
    if wavelabels is None:
        wavelabels = []
    if numericlabels is None:
        numericlabels = []
    if numericsublabels is None:
        numericsublabels = []
    q = build_query(
        uri,
        patientid,
        name,
        firstname,
        ipp,
        dtbegin,
        dtend,
        clinicalunit,
        bedlabel,
        wavelabels,
        numericlabels,
        numericsublabels,
    )
    if limit:
        q = q.limit(limit)
    engine = create_engine(uri)
    with engine.connect() as conn:
        try:
            df = pd.read_sql(q, conn, index_col="patientid")
        except sqlalchemy.exc.DataError:
            df = pd.DataFrame([])
    return df


def read_patient_dwc_native(*args, **kwargs) -> Optional[pd.Series]:
    """Reads a single patient from DWC database with the need of dwcmeta.

    Retrieves a pandas series consisting of the patient which matches the search criteria.

    Args:
        patientid: A DWC patient identifier
        dtbegin: The beginning of available data samples
        dtend: The end of available data samples
        clinicalunit: The clinical unit the patient belongs to
        bedlabel: The bed / room in which the patient stays
        uri: Optional sqlalchemy URI for the database if not provided in the config file

    Returns:
        A pandas series corresponding to a patient stay.
    """  # noqa: DAR101,DAR102
    res = read_patients_dwc_native(*args, **kwargs, limit=1)
    if len(res):
        return res.iloc[0]


def read_patients_dwc_native(
    patientid: str = None,
    dtbegin: str = None,
    dtend: str = None,
    clinicalunit: str = None,
    bedlabel: str = None,
    uri: Optional[str] = None,
    limit: Optional[int] = None,
) -> pd.DataFrame:
    """Reads patients from DWC database with the need of dwcmeta.

    Retrieves a pandas dataframe consisting of individual patients which match the search criteria.

    Args:
        patientid: A DWC patient identifier
        dtbegin: The beginning of available data samples
        dtend: The end of available data samples
        clinicalunit: The clinical unit the patient belongs to
        bedlabel: The bed / room in which the patient stays
        uri: Optional sqlalchemy URI for the database if not provided in the config file
        limit: Maximum number of returned results

    Returns:
        A pandas dataframe with each row corresponding to an individual patient stay, indexed by unique DWC patient identifiers.
    """
    if not uri:
        uri = dwcuri

    q = build_query_dwc_native(
        uri,
        patientid,
        dtbegin,
        dtend,
        clinicalunit,
        bedlabel,
    )

    if limit:
        q = q.limit(limit)
    engine = create_engine(uri)
    with engine.connect() as conn:
        df = pd.read_sql(q, conn, index_col="patientid")
    return df


def build_query(
    uri,
    patientid,
    name,
    firstname,
    ipp,
    dtbegin,
    dtend,
    clinicalunit,
    bedlabel,
    wavelabels,
    numericlabels,
    numericsublabels,
):
    pgdb = create_engine(uri)
    pgmeta = MetaData()
    t_patients = Table("patients", pgmeta, autoload_with=pgdb)
    p = t_patients.c
    t_patientlabels = Table("patientlabels", pgmeta, autoload_with=pgdb)
    pl = t_patientlabels.c

    if name or firstname:
        # Build a subquery to allow approximate name search
        fields = [t_patients]
        if name:
            fields.append(
                func.levenshtein(func.lower(p.lastname), func.lower(name)).label(
                    "d_name"
                )
            )
        if firstname:
            fields.append(
                func.levenshtein(func.lower(p.firstname), func.lower(firstname)).label(
                    "d_firstname"
                )
            )
        t_patients = select(*fields).cte()  # Replace Patients table with CTE
        p = t_patients.c

    q = select(t_patients, pl.numericlabels, pl.numericsublabels, pl.wavelabels)
    q = q.select_from(
        t_patients.join(t_patientlabels, pl.patientid == p.patientid, isouter=True)
    )
    if patientid:
        q = q.where(p.patientid == patientid)
    if name:
        q = q.where(p.d_name < 3)
        q = q.order_by(asc(p.d_name))
    if firstname:
        q = q.where(p.d_firstname < 3)
        q = q.order_by(asc(p.d_firstname))
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
    if numericsublabels:
        q = q.where(pl.numericsublabels.contains(numericsublabels))
    if wavelabels:
        q = q.where(pl.wavelabels.contains(wavelabels))
    return q


def build_query_dwc_native(
    uri,
    patientid,
    dtbegin,
    dtend,
    clinicalunit,
    bedlabel,
):
    dwcdb = create_engine(uri)
    meta = MetaData(schema="_Export")
    t_patients = Table("Patient_", meta, autoload_with=dwcdb)
    p = t_patients.c

    q = select(
        p.Id.label("patientid"),
        func.min(p.Timestamp).label("data_begin"),
        func.max(p.Timestamp).label("data_end"),
        func.max(p.BedLabel).label("bedlabel"),
        func.max(p.AdmitState).label("admitstate"),
        func.max(p.ClinicalUnit).label("clinicalunit"),
        func.max(p.Gender).label("gender"),
    )
    q = q.group_by(p.Id)

    if patientid:
        q = q.where(p.Id == patientid)
    if bedlabel:
        q = q.where(p.BedLabel == bedlabel)
    if clinicalunit:
        q = q.where(p.ClinicalUnit == clinicalunit)
    if dtbegin:
        q = q.where(p.Timestamp >= dtbegin)
    if dtend:
        q = q.where(p.Timestamp <= dtend)
    return q
