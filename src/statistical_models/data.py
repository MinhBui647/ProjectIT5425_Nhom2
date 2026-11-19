"""Đọc và chuẩn bị dữ liệu Gold cho các mô hình của TV6.

- Đặc trưng theo lô: G batch_features.parquet, cộng thêm homo_press1_avg gộp từ G fact_sensor_telemetry
  (dùng lại manufacturing_spc.data_access.load_from_parquet của TV5 để hai phân hệ cùng một cách gộp).
- Gán group_id cho các lô trùng 5 giá trị MES, khử trùng để tránh rò rỉ train/test.
- Trung bình theo phút của từng lô cho Bài 3.
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

def assign_groups(batches: pd.DataFrame, keys: tuple[str, ...] = cfg.DEDUP_KEYS) -> pd.DataFrame:
    """Gán group_id: các lô có cùng 5 giá trị MES là cùng một lô (bản sao)."""
    b = batches.copy()
    k = b[list(keys)].round(6)
    b[cfg.GROUP_COL] = k.groupby(list(keys), sort=False).ngroup().to_numpy()
    return b

def deduplicate(batches: pd.DataFrame) -> pd.DataFrame:
    """Mỗi group_id giữ một lô (lô có batch_key nhỏ nhất)."""
    return (batches.sort_values("batch_key").drop_duplicates(cfg.GROUP_COL, keep="first")
            .reset_index(drop=True))

def fill_market_gaps(batches: pd.DataFrame, cols: tuple[str, ...] = ("gdt_smp_usd", "gdt_wmp_usd")) -> pd.DataFrame:
    """Giá GDT là hàm bậc thang theo phiên đấu giá: thiếu thì lấy phiên gần nhất trước đó,
    các ngày trước phiên đầu tiên trong dữ liệu thì lấy phiên đầu tiên."""
    b = batches.copy()
    order = b.sort_values(["date_key", "batch_key"]).index
    for c in cols:
        if c in b.columns:
            b.loc[order, c] = b.loc[order, c].ffill().bfill()
    return b

def usable_features(df: pd.DataFrame, features: tuple[str, ...]) -> tuple[list[str], list[dict]]:
    """Bỏ biến không có trong dữ liệu, toàn NaN hoặc hằng số; trả về (biến dùng được, lý do bỏ)."""
    keep, dropped = [], []
    for f in features:
        if f not in df.columns:
            dropped.append({"feature": f, "reason": "không có trong batch_features"})
        elif df[f].isna().all():
            dropped.append({"feature": f, "reason": "toàn NaN"})
        elif df[f].nunique(dropna=True) <= 1:
            dropped.append({"feature": f, "reason": "hằng số"})
        else:
            keep.append(f)
    return keep, dropped

def prepare_batches(batches: pd.DataFrame) -> dict:
    """Bước chuẩn bị chung. Trả về dict gồm bảng 240 lô (có group_id) và bảng đã khử trùng."""
    b = fill_market_gaps(assign_groups(batch_scope(batches)))
    d = deduplicate(b)
    return {
        "all": b,
        "dedup": d,
        "summary": {
            "n_batches": int(len(b)), "n_groups": int(d.shape[0]),
            "n_defect_batches": int(b[cfg.DEFECT_TARGET].sum()),
            "n_defect_groups": int(d[cfg.DEFECT_TARGET].sum()),
            "group_size_counts": {int(k): int(v) for k, v in b.groupby(cfg.GROUP_COL).size().value_counts()
                                  .sort_index().items()},
        },
    }

def minute_means(sensor: pd.DataFrame, year: int = cfg.YEAR,
                 cols: tuple[str, ...] = cfg.SENSOR_VARS) -> pd.DataFrame:
    """Trung bình theo phút của từng lô (Bài 3)."""
    s = sensor.loc[sensor["batch_key"].notna(), ["batch_key", "timestamp", *cols]].copy()
    s["timestamp"] = pd.to_datetime(s["timestamp"])
    s = s[s["timestamp"].dt.year == year]
    s["minute"] = s["timestamp"].dt.floor("min")
    g = s.groupby(["batch_key", "minute"], sort=True)
    out = g[list(cols)].mean().astype("float64")
    out["n"] = g.size()
    return out.reset_index()

def zscore(X: np.ndarray, mean: np.ndarray | None = None, sd: np.ndarray | None = None):
    """Chuẩn hóa z-score theo cột (bỏ qua NaN). Trả về (Z, mean, sd)."""
    mean = np.nanmean(X, axis=0) if mean is None else mean
    sd = np.nanstd(X, axis=0, ddof=0) if sd is None else sd
    sd = np.where(sd > 0, sd, 1.0)
    return (X - mean) / sd, mean, sd

def group_folds(groups: np.ndarray, n_splits: int, seed: int) -> list[tuple[np.ndarray, np.ndarray]]:
    """K-fold theo nhóm có xáo trộn: mỗi nhóm nằm trọn trong một fold, đổi seed là đổi cách chia."""
    groups = np.asarray(groups)
    uniq = np.unique(groups)
    k = min(n_splits, len(uniq))
    perm = np.random.default_rng(seed).permutation(uniq)
    fold_of = dict(zip(perm, np.arange(len(perm)) % k))
    f = np.array([fold_of[g] for g in groups])
    return [(np.flatnonzero(f != i), np.flatnonzero(f == i)) for i in range(k)]

def split_by_group(groups: np.ndarray, valid_fraction: float, seed: int) -> tuple[np.ndarray, np.ndarray]:
    """Tách train/valid theo nhóm (ví dụ batch_key). Trả về mặt nạ bool (train, valid)."""
    groups = np.asarray(groups)
    uniq = np.random.default_rng(seed).permutation(np.unique(groups))
    n_valid = max(1, int(round(valid_fraction * len(uniq))))
    valid = np.isin(groups, uniq[:n_valid])
    return ~valid, valid