import pandas as pd


def build_numerics_meta():
    idx = pd.DatetimeIndex([], name="TimeStamp", tz="UTC")
    dtypes = {
        "PatientId": "string",
        "Label": "string",
        "SubLabel": "string",
        "Value": "float32",
    }
    mdf = pd.DataFrame({k: [] for k in dtypes.keys()}, index=idx)
    mdf = mdf.astype(dtype=dtypes)
    return mdf


def build_waves_meta():
    idx = pd.DatetimeIndex([], name="TimeStamp", tz="UTC")
    dtypes = {
        "PatientId": "string",
        "Label": "string",
        "WaveSamples": "object",
        "SamplePeriod": "int32",
        "CAU": "float64",
        "CAL": "float64",
        "CSU": "int32",
        "CSL": "int32",
    }
    mdf = pd.DataFrame({k: [] for k in dtypes.keys()}, index=idx)
    mdf = mdf.astype(dtype=dtypes)
    return mdf


def build_enumerations_meta():
    idx = pd.DatetimeIndex([], name="TimeStamp", tz="UTC")
    dtypes = {
        "PatientId": "string",
        "Label": "string",
        "Value": "string",
    }
    mdf = pd.DataFrame({k: [] for k in dtypes.keys()}, index=idx)
    mdf = mdf.astype(dtype=dtypes)
    return mdf


def build_patients_meta():
    idx = pd.Index([], dtype=object, name="PatientId")
    dtypes = {
        "PatientId": "string",
        "Label": "string",
        "Value": "string",
    }
    mdf = pd.DataFrame({k: [] for k in dtypes.keys()}, index=idx)
    mdf = mdf.astype(dtype=dtypes)
    return mdf


numerics_meta = build_numerics_meta()
waves_meta = build_waves_meta()
enumerations_meta = build_enumerations_meta()
