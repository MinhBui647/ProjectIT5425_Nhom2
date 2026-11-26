"""Đọc dữ liệu Gold cho các mô hình của TV6.

- Đọc bằng manufacturing_spc.data_access.load_from_parquet của TV5.
- Trung bình theo phút của từng lô, chia train/valid theo lô.
"""

from __future__ import annotations
from pathlib import Path

import numpy as np
import pandas as pd

from . import config as cfg

def load_gold(sensor_path: str | Path, features_path: str | Path) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Trả về (cảm biến 1 Hz, đặc trưng theo lô). Không cần chDB hay Docker."""
    from manufacturing_spc.data_access import load_from_parquet

    return load_from_parquet(sensor_path, features_path)

def _year(df: pd.DataFrame) -> pd.Series:
    if "date_key" in df.columns:
        return (pd.to_numeric(df["date_key"]) // 10000).astype(int)
    return pd.to_datetime(df["full_date"]).dt.year

def batch_scope(batches: pd.DataFrame, year: int = cfg.YEAR) -> pd.DataFrame:
    """Lô năm `year` có n_readings > 0, các cột số ép về float64 (Gold lưu float32)."""
    b = batches[(_year(batches) == year)].copy()
    if "n_readings" in b.columns:
        b = b[b["n_readings"] > 0]
    num = b.select_dtypes("number").columns
    b[num] = b[num].astype("float64")
    return b.sort_values("batch_key").reset_index(drop=True)

def minute_means(sensor: pd.DataFrame, year: int = cfg.YEAR,
                 cols: tuple[str, ...] = cfg.SENSOR_VARS) -> pd.DataFrame:
    """Trung bình theo phút của từng lô."""
    s = sensor.loc[sensor["batch_key"].notna(), ["batch_key", "timestamp", *cols]].copy()
    s["timestamp"] = pd.to_datetime(s["timestamp"])
    s = s[s["timestamp"].dt.year == year]
    s["minute"] = s["timestamp"].dt.floor("min")
    g = s.groupby(["batch_key", "minute"], sort=True)
    out = g[list(cols)].mean().astype("float64")
    out["n"] = g.size()
    return out.reset_index()

def split_by_group(groups: np.ndarray, valid_fraction: float, seed: int) -> tuple[np.ndarray, np.ndarray]:
    """Tách train/valid theo nhóm (ví dụ batch_key). Trả về mặt nạ bool (train, valid)."""
    groups = np.asarray(groups)
    uniq = np.random.default_rng(seed).permutation(np.unique(groups))
    n_valid = max(1, int(round(valid_fraction * len(uniq))))
    valid = np.isin(groups, uniq[:n_valid])
    return ~valid, valid