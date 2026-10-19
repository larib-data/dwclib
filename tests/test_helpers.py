"""Pure helpers: wave decoding, id squeezing, time coercion."""

from datetime import datetime

import numpy as np
import pandas as pd
import pytest

from dwclib.common.dt import to_datetime
from dwclib.common.ids import squeeze_ids
from dwclib.common.wave_unfold import unfold_row, wave_unfold


def _int16le(values) -> bytes:
    return np.asarray(values, dtype="<i2").tobytes()


def test_wave_unfold_applies_calibration():
    # Scaled 0..1000 maps to -1..1 mV.
    out = wave_unfold(_int16le([0, 500, 1000]), 1.0, -1.0, 1000, 0)
    np.testing.assert_allclose(out, [-1.0, 0.0, 1.0])
    assert out.dtype == np.float64 or out.dtype == np.float32


def test_wave_unfold_without_calibration_is_identity():
    raw = _int16le([-3, 0, 7])
    np.testing.assert_array_equal(wave_unfold(raw, np.nan, 0, 0, 0), [-3, 0, 7])
    # csu == csl would divide by zero: identity too.
    np.testing.assert_array_equal(wave_unfold(raw, 1.0, 0.0, 5, 5), [-3, 0, 7])


def test_wave_unfold_drops_a_trailing_odd_byte():
    assert len(wave_unfold(_int16le([1, 2]) + b"\x01", 1, 0, 1, 0)) == 2


def test_unfold_row_builds_a_millisecond_index():
    t0 = pd.Timestamp("2026-01-01 08:00", tz="UTC")
    row = pd.Series(
        {
            "SamplePeriod": 4,
            "WaveSamples": _int16le([10, 20, 30]),
            "CAU": None,
            "CAL": None,
            "CSU": None,
            "CSL": None,
        },
        name=t0,
    )
    out = unfold_row(row)
    assert list(out) == [10, 20, 30]
    assert list(out.index) == [t0 + pd.Timedelta(milliseconds=4 * i) for i in range(3)]
    # A (TimeStamp, PatientId) MultiIndex row takes its time from the first level.
    row.name = (t0, "pid")
    assert unfold_row(row).index[0] == t0


@pytest.mark.parametrize(
    ("ids", "expected"),
    [(None, None), ("a", "a"), (["a"], "a"), (("a",), "a"), (["a", "b"], ["a", "b"])],
)
def test_squeeze_ids(ids, expected):
    assert squeeze_ids(ids) == expected


def test_to_datetime():
    assert to_datetime(None) is None
    assert to_datetime("2026-01-01T08:00:00") == datetime(2026, 1, 1, 8)
    value = to_datetime("2026-01-01T08:00:00+00:00")
    assert type(value) is datetime and value.tzinfo is not None
    with pytest.raises(ValueError):
        to_datetime("not a date")
