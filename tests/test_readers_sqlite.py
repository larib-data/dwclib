"""The DWC readers end to end on a SQLite copy of the ``_Export`` tables.

A SQLite database is attached as the ``_Export`` schema on every connection,
so the readers' table reflection and queries run unchanged (the MSSQL
``WITH (NOLOCK)`` hints are not rendered for SQLite).
"""

from datetime import datetime, timedelta

import numpy as np
import pandas as pd
import pytest
import sqlalchemy as sa
import sqlalchemy.event

from dwclib import read_numerics, read_waves
from dwclib.patients.patients import build_query_dwc_native

T0 = datetime(2026, 1, 1, 8, 0)
PID, OTHER = "pid-a", "pid-b"


@pytest.fixture
def dwc(tmp_path):
    """A SQLite URI whose connections see the DWC ``_Export`` tables."""
    export_db = str(tmp_path / "export.db")

    def attach(dbapi_conn, _record):
        dbapi_conn.execute(f"ATTACH DATABASE '{export_db}' AS \"_Export\"")

    sa.event.listen(sa.engine.Engine, "connect", attach)
    try:
        meta = sa.MetaData(schema="_Export")
        sa.Table(
            "Numeric_",
            meta,
            sa.Column("Id", sa.Integer),
            sa.Column("TimeStamp", sa.DateTime),
            sa.Column("Label", sa.String),
            sa.Column("SubLabel", sa.String),
        )
        sa.Table(
            "NumericValue_",
            meta,
            sa.Column("NumericId", sa.Integer),
            sa.Column("TimeStamp", sa.DateTime),
            sa.Column("PatientId", sa.String),
            sa.Column("Value", sa.Float),
        )
        sa.Table(
            "Wave_",
            meta,
            sa.Column("Id", sa.Integer),
            sa.Column("TimeStamp", sa.DateTime),
            sa.Column("Label", sa.String),
            sa.Column("SamplePeriod", sa.Integer),
            sa.Column("CalibrationAbsUpper", sa.Float),
            sa.Column("CalibrationAbsLower", sa.Float),
            sa.Column("CalibrationScaledUpper", sa.Integer),
            sa.Column("CalibrationScaledLower", sa.Integer),
        )
        sa.Table(
            "WaveSample_",
            meta,
            sa.Column("WaveId", sa.Integer),
            sa.Column("TimeStamp", sa.DateTime),
            sa.Column("PatientId", sa.String),
            sa.Column("WaveSamples", sa.LargeBinary),
        )
        sa.Table(
            "Patient_",
            meta,
            sa.Column("Id", sa.String),
            sa.Column("Timestamp", sa.DateTime),
            sa.Column("BedLabel", sa.String),
            sa.Column("AdmitState", sa.Integer),
            sa.Column("ClinicalUnit", sa.String),
            sa.Column("Gender", sa.String),
        )
        uri = f"sqlite:///{tmp_path / 'main.db'}"
        engine = sa.create_engine(uri)
        meta.create_all(engine)
        t = meta.tables
        with engine.begin() as conn:
            # Numeric rows: a new Id each minute, as in DWC (values join on Id).
            for minute in range(3):
                ts = T0 + timedelta(minutes=minute)
                conn.execute(
                    t["_Export.Numeric_"].insert(),
                    [
                        {
                            "Id": 10 * minute + 1,
                            "TimeStamp": ts,
                            "Label": "ART",
                            "SubLabel": "ARTm",
                        },
                        {
                            "Id": 10 * minute + 2,
                            "TimeStamp": ts,
                            "Label": "FC",
                            "SubLabel": "FC",
                        },
                    ],
                )
                conn.execute(
                    t["_Export.NumericValue_"].insert(),
                    [
                        {
                            "NumericId": 10 * minute + 1,
                            "TimeStamp": ts,
                            "PatientId": PID,
                            "Value": 70 + minute,
                        },
                        {
                            "NumericId": 10 * minute + 2,
                            "TimeStamp": ts,
                            "PatientId": PID,
                            "Value": 60.0,
                        },
                        {
                            "NumericId": 10 * minute + 1,
                            "TimeStamp": ts,
                            "PatientId": OTHER,
                            "Value": 99.0,
                        },
                        {
                            "NumericId": 10 * minute + 2,
                            "TimeStamp": ts,
                            "PatientId": PID,
                            "Value": None,
                        },
                    ],
                )
            conn.execute(
                t["_Export.Wave_"].insert(),
                [
                    {
                        "Id": 7,
                        "TimeStamp": T0,
                        "Label": "II",
                        "SamplePeriod": 4,
                        "CalibrationAbsUpper": 1.0,
                        "CalibrationAbsLower": -1.0,
                        "CalibrationScaledUpper": 1000,
                        "CalibrationScaledLower": 0,
                    }
                ],
            )
            conn.execute(
                t["_Export.WaveSample_"].insert(),
                [
                    {
                        "WaveId": 7,
                        "TimeStamp": T0,
                        "PatientId": PID,
                        "WaveSamples": np.array([0, 500, 1000], "<i2").tobytes(),
                    }
                ],
            )
            conn.execute(
                t["_Export.Patient_"].insert(),
                [
                    {
                        "Id": PID,
                        "Timestamp": T0,
                        "BedLabel": "BLOC3",
                        "ClinicalUnit": "BLOC",
                    },
                    {
                        "Id": PID,
                        "Timestamp": T0 + timedelta(hours=2),
                        "BedLabel": "BLOC3",
                        "ClinicalUnit": "BLOC",
                    },
                ],
            )
        engine.dispose()
        yield uri
    finally:
        sa.event.remove(sa.engine.Engine, "connect", attach)


def test_read_numerics_long(dwc):
    df = read_numerics(PID, T0, T0 + timedelta(minutes=2), pivot=False, uri=dwc)
    # Half-open window: minutes 0 and 1; null values and the other patient dropped.
    assert len(df) == 4
    assert set(df["PatientId"]) == {PID}
    assert str(pd.DatetimeIndex(df.index).tz) == "UTC"
    assert df["Value"].dtype == np.float32
    art = df[df["SubLabel"] == "ARTm"]["Value"].tolist()
    assert art == [70.0, 71.0]


def test_read_numerics_pivot_and_filters(dwc):
    df = read_numerics([PID], T0, T0 + timedelta(minutes=3), labels=["ART"], uri=dwc)
    # A one-element list is a single patient: the PatientId level is dropped.
    assert list(df.columns) == ["ARTm"]
    assert df["ARTm"].tolist() == [70.0, 71.0, 72.0]
    both = read_numerics(
        [PID, OTHER], T0, T0 + timedelta(minutes=1), sublabels=["ARTm"], uri=dwc
    )
    assert set(both.columns) == {(PID, "ARTm"), (OTHER, "ARTm")}


def test_read_numerics_empty_window(dwc):
    df = read_numerics(PID, T0 - timedelta(hours=1), T0, uri=dwc)
    assert df.empty
    assert list(df.columns) == ["PatientId", "Label", "SubLabel", "Value"]


def test_read_waves_unfolds_and_calibrates(dwc):
    df = read_waves(PID, T0, T0 + timedelta(seconds=1), uri=dwc)
    assert list(df.columns) == ["II"]
    np.testing.assert_allclose(df["II"].to_numpy(), [-1.0, 0.0, 1.0])
    start = pd.Timestamp(T0, tz="UTC")
    assert list(df.index) == [
        start + pd.Timedelta(milliseconds=4 * i) for i in range(3)
    ]
    assert read_waves(OTHER, T0, T0 + timedelta(seconds=1), uri=dwc).empty


def test_native_patient_query(dwc):
    q = build_query_dwc_native(dwc, PID, T0, None, "BLOC", None)
    with sa.create_engine(dwc).connect() as conn:
        rows = conn.execute(q).mappings().all()
    assert len(rows) == 1
    row = rows[0]
    assert (row["patientid"], row["bedlabel"], row["clinicalunit"]) == (
        PID,
        "BLOC3",
        "BLOC",
    )
    assert pd.Timestamp(row["data_end"]) - pd.Timestamp(row["data_begin"]) == (
        pd.Timedelta(hours=2)
    )
