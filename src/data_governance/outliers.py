"""Sensor outlier detection that flags spikes without changing measurements."""

from __future__ import annotations

import math
import statistics
from collections.abc import Sequence

import polars as pl

from data_governance.config import (
    HAMPEL_THRESHOLD,
    HAMPEL_WINDOW_RADIUS,
    ROLLING_Z_THRESHOLD,
    ROLLING_Z_WINDOW,
)


def _finite(values: Sequence[float | None]) -> list[float]:
    return [
        float(value)
        for value in values
        if value is not None and math.isfinite(float(value))
    ]


def hampel_flags(
    values: Sequence[float | None],
    *,
    window_radius: int = HAMPEL_WINDOW_RADIUS,
    threshold: float = HAMPEL_THRESHOLD,
) -> list[bool]:
    """Return centered Hampel flags using the scaled median absolute deviation."""
    if window_radius < 1:
        raise ValueError("window_radius must be at least 1")
    flags = [False] * len(values)
    for index, raw_value in enumerate(values):
        if raw_value is None or not math.isfinite(float(raw_value)):
            continue
        start = max(0, index - window_radius)
        stop = min(len(values), index + window_radius + 1)
        window = _finite(values[start:stop])
        if len(window) < 3:
            continue
        median = statistics.median(window)
        mad = statistics.median(abs(value - median) for value in window)
        deviation = abs(float(raw_value) - median)
        if mad == 0:
            flags[index] = deviation > 0
        else:
            flags[index] = deviation > threshold * 1.4826 * mad
    return flags


def rolling_zscore_flags(
    values: Sequence[float | None],
    *,
    window: int = ROLLING_Z_WINDOW,
    threshold: float = ROLLING_Z_THRESHOLD,
) -> list[bool]:
    """Return trailing rolling population Z-score flags, including each row."""
    if window < 3:
        raise ValueError("rolling window must be at least 3")
    flags = [False] * len(values)
    for index, raw_value in enumerate(values):
        if raw_value is None or not math.isfinite(float(raw_value)):
            continue
        start = max(0, index - window + 1)
        sample = _finite(values[start : index + 1])
        if len(sample) < 3:
            continue
        deviation = statistics.pstdev(sample)
        if deviation == 0:
            continue
        z_score = (float(raw_value) - statistics.fmean(sample)) / deviation
        flags[index] = abs(z_score) > threshold
    return flags


def flag_sensor_outliers(
    df: pl.DataFrame,
    *,
    value_columns: tuple[str, ...] = ("uht_temp",),
    window_radius: int = HAMPEL_WINDOW_RADIUS,
    hampel_threshold: float = HAMPEL_THRESHOLD,
    rolling_window: int = ROLLING_Z_WINDOW,
    rolling_threshold: float = ROLLING_Z_THRESHOLD,
) -> pl.DataFrame:
    """Add ``is_anomaly_flag`` from the union of Hampel and rolling-Z signals.

    Calculations are isolated by ``line_id`` and ordered by ``timestamp``. The
    caller's row order and every existing value are preserved.
    """
    required = {"line_id", "timestamp", *value_columns}
    missing = sorted(required - set(df.columns))
    if missing:
        raise ValueError(f"Missing sensor outlier columns: {missing}")

    indexed = df.with_row_index("_governance_row_index")
    ordered = indexed.sort("line_id", "timestamp")
    flagged_groups: list[pl.DataFrame] = []
    for group in ordered.partition_by("line_id", maintain_order=True):
        group_flags = [False] * group.height
        for column in value_columns:
            values = group.get_column(column).to_list()
            hampel = hampel_flags(
                values,
                window_radius=window_radius,
                threshold=hampel_threshold,
            )
            rolling = rolling_zscore_flags(
                values,
                window=rolling_window,
                threshold=rolling_threshold,
            )
            group_flags = [
                current or hampel_flag or rolling_flag
                for current, hampel_flag, rolling_flag in zip(
                    group_flags,
                    hampel,
                    rolling,
                    strict=True,
                )
            ]
        flagged_groups.append(
            group.with_columns(
                pl.Series("is_anomaly_flag", group_flags, dtype=pl.Boolean)
            )
        )

    if not flagged_groups:
        return df.with_columns(
            pl.Series("is_anomaly_flag", [], dtype=pl.Boolean)
        )
    return (
        pl.concat(flagged_groups)
        .sort("_governance_row_index")
        .drop("_governance_row_index")
    )

