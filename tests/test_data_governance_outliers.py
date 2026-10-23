"""Tests for Sensor Hampel and rolling Z-score outlier detection."""

from datetime import datetime, timedelta, timezone

import polars as pl
from polars.testing import assert_frame_equal

from data_governance.outliers import (
    flag_sensor_outliers,
    hampel_flags,
    rolling_zscore_flags,
)


def _sensor_frame(lines_and_values):
    base = datetime(2026, 1, 1, tzinfo=timezone.utc)
    rows = []
    for line_id, values in lines_and_values:
        rows.extend(
            {
                "line_id": line_id,
                "timestamp": base + timedelta(seconds=index),
                "uht_temp": value,
                "_bronze_sha256": "a" * 64,
            }
            for index, value in enumerate(values)
        )
    return pl.DataFrame(rows).with_columns(
        pl.col("timestamp").cast(pl.Datetime("us", "UTC")),
        pl.col("uht_temp").cast(pl.Float32),
    )


def test_hampel_flags_single_spike_when_mad_is_zero():
    values = [140.0] * 21
    values[10] = 160.0

    flags = hampel_flags(values)

    assert [index for index, flagged in enumerate(flags) if flagged] == [10]


def test_rolling_zscore_flags_spike_after_constant_history():
    values = [140.0] * 29 + [170.0]

    flags = rolling_zscore_flags(values)

    assert flags[-1] is True
    assert sum(flags) == 1


def test_constant_short_and_null_series_are_safe():
    assert not any(hampel_flags([140.0] * 20))
    assert not any(rolling_zscore_flags([140.0] * 20))
    assert hampel_flags([140.0, None]) == [False, False]
    assert rolling_zscore_flags([None, 140.0]) == [False, False]


def test_sensor_flags_are_grouped_by_line_and_do_not_mutate_values_or_input():
    line_a = [140.0] * 21
    line_a[10] = 160.0
    frame = _sensor_frame(
        [
            ("LINE_A", line_a),
            ("LINE_B", [140.0] * 21),
        ]
    ).reverse()
    original = frame.clone()

    flagged = flag_sensor_outliers(frame)

    anomalies = flagged.filter(pl.col("is_anomaly_flag"))
    assert anomalies.height == 1
    assert anomalies["line_id"].to_list() == ["LINE_A"]
    assert anomalies["uht_temp"].to_list() == [160.0]
    assert flagged["uht_temp"].to_list() == frame["uht_temp"].to_list()
    assert flagged["_bronze_sha256"].to_list() == frame["_bronze_sha256"].to_list()
    assert_frame_equal(frame, original)


def test_null_measurement_is_never_marked_as_anomaly():
    frame = _sensor_frame([("LINE_A", [140.0] * 10 + [None] + [140.0] * 10)])

    flagged = flag_sensor_outliers(frame)

    assert flagged.filter(pl.col("uht_temp").is_null())["is_anomaly_flag"].to_list() == [False]

