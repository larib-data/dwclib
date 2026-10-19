import subprocess
import sys
from datetime import datetime

import pandas as pd
import pytest
import sqlalchemy as sa

from dwclib.common import db
from dwclib.numerics import numerics
from dwclib.patients.patients import build_query


def test_import_does_not_pull_dask():
    code = "import sys, dwclib; assert not any(m.split('.')[0] == 'dask' for m in sys.modules)"
    subprocess.run([sys.executable, "-c", code], check=True)


def test_pandas_3():
    assert int(pd.__version__.split(".")[0]) >= 3


@pytest.fixture
def pg_like(tmp_path):
    """A SQLite file with the DWCmeta tables used by build_query."""
    uri = f"sqlite:///{tmp_path / 'meta.db'}"
    meta = sa.MetaData()
    sa.Table(
        "patients",
        meta,
        sa.Column("patientid", sa.String, primary_key=True),
        sa.Column("lifetimeid", sa.String),
        sa.Column("encounterid", sa.String),
        sa.Column("bedlabel", sa.String),
        sa.Column("clinicalunit", sa.String),
        sa.Column("data_begin", sa.DateTime),
        sa.Column("data_end", sa.DateTime),
    )
    sa.Table(
        "patientlabels",
        meta,
        sa.Column("patientid", sa.String),
        sa.Column("numericlabels", sa.String),
        sa.Column("numericsublabels", sa.String),
        sa.Column("wavelabels", sa.String),
    )
    meta.create_all(sa.create_engine(uri))
    return uri


def _build(uri, **kw):
    args = dict.fromkeys(
        [
            "patientid",
            "name",
            "firstname",
            "ipp",
            "nda",
            "dtbegin",
            "dtend",
            "clinicalunit",
            "bedlabel",
        ]
    )
    args.update(kw)
    return build_query(
        uri, **args, wavelabels=[], numericlabels=[], numericsublabels=[]
    )


def test_read_patients_nda_filter(pg_like):
    q = _build(pg_like, nda="70001")
    sql = str(q.compile(compile_kwargs={"literal_binds": True}))
    assert "patients.encounterid = '70001'" in sql
    assert "encounterid" in [c.name for c in q.selected_columns]


def test_read_patients_ipp_and_nda(pg_like):
    q = _build(pg_like, ipp="9000000017", nda="70001")
    sql = str(q.compile(compile_kwargs={"literal_binds": True}))
    assert "patients.lifetimeid = '9000000017'" in sql
    assert "patients.encounterid = '70001'" in sql


def test_update_config_is_seen_at_call_time(tmp_path, monkeypatch):
    monkeypatch.setattr(db, "configfile", tmp_path / "config.ini")
    monkeypatch.setattr(db, "dwcuri", None)
    monkeypatch.setattr(db, "pguri", None)
    db.update_config("[dwc]\nuser = u\npass = p\nhost = newhost\ndbname = d\n")
    seen = {}

    def fake_query(uri, *args):
        seen["uri"] = uri
        return pd.DataFrame({"Value": []})

    monkeypatch.setattr(numerics, "run_numerics_query", fake_query)
    numerics.read_numerics("pid", datetime(2024, 1, 1), datetime(2024, 1, 2))
    assert seen["uri"].host == "newhost"
