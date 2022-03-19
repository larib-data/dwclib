from pandas.api.types import is_list_like
from sqlalchemy import MetaData, Table, join, select


def build_numerics_query(engine, dtbegin, dtend, patientids=[], labels=[]):
    dbmeta = MetaData(bind=engine)
    nnt = Table(
        'Numeric_', dbmeta, schema='_Export', autoload=True, autoload_with=engine
    )
    nvt = Table(
        'NumericValue_', dbmeta, schema='_Export', autoload=True, autoload_with=engine
    )

    nn = select(nnt.c.TimeStamp, nnt.c.Id, nnt.c.SubLabel)
    nn = nn.where(nnt.c.TimeStamp >= dtbegin)
    nn = nn.where(nnt.c.TimeStamp < dtend)
    if labels:
        nn = nn.where(nnt.c.SubLabel.in_(labels))
    nn = nn.cte('Numeric')

    nv = select(nvt.c.TimeStamp, nvt.c.NumericId, nvt.c.Value, nvt.c.PatientId)
    nv = nv.where(nvt.c.TimeStamp >= dtbegin)
    nv = nv.where(nvt.c.TimeStamp < dtend)
    nv = nv.where(nvt.c.Value is not None)
    if patientids:
        if is_list_like(patientids):
            nv = nv.where(nvt.c.PatientId.in_(patientids))
        else:
            nv = nv.where(nvt.c.PatientId.in_ == patientids)
    nv = nv.cte('NumericValue')

    j = join(nv, nn, nv.c.NumericId == nn.c.Id)
    q = select(nv.c.TimeStamp, nv.c.PatientId, nn.c.SubLabel, nv.c.Value).select_from(j)
    return q
