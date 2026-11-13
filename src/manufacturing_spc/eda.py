"""EDA công nghiệp: bảng thống kê mô tả và kiểm tra phân phối (TV5)."""

from __future__ import annotations

import numpy as np
import pandas as pd
from scipy import stats


def eda_summary(df: pd.DataFrame, cols: list[str], source: str) -> pd.DataFrame:
    """Mỗi dòng là một biến: số mẫu, % thiếu, trung bình, độ lệch chuẩn, phân vị, độ lệch/nhọn, kiểm định chuẩn, % ngoại lai IQR."""
    rows = []
    for c in cols:
        x = pd.to_numeric(df[c], errors="coerce")
        v = x.dropna().to_numpy(float)
        if len(v) < 8:
            continue  # quá ít điểm thì thống kê không có nghĩa
        p1, p5, q1, med, q3, p95, p99 = np.percentile(v, [1, 5, 25, 50, 75, 95, 99])
        iqr = q3 - q1
        # kiểm định chuẩn lấy tối đa 5000 điểm, nhiều hơn thì nó bắt lỗi chuẩn cả những lệch rất nhỏ
        sample = v if len(v) <= 5000 else np.random.default_rng(0).choice(v, 5000, replace=False)
        constant = np.ptp(v) == 0
        rows.append({
            "source": source, "variable": c, "n": len(v), "missing_pct": float(x.isna().mean() * 100),
            "mean": v.mean(), "std": v.std(ddof=1), "min": v.min(), "p1": p1, "p5": p5, "q1": q1, "median": med, "q3": q3, "p95": p95, "p99": p99,
            "max": v.max(),
            "skewness": np.nan if constant else float(stats.skew(v)),
            "excess_kurtosis": np.nan if constant else float(stats.kurtosis(v)),
            "normality_p": np.nan if constant else float(stats.normaltest(sample).pvalue),
            "outliers_3sigma_n": 0 if constant else int((np.abs(v - v.mean()) > 3 * v.std(ddof=1)).sum()),
            "outliers_3sigma_pct": 0.0 if constant else float((np.abs(v - v.mean()) > 3 * v.std(ddof=1)).mean() * 100),
            "outliers_iqr_n": int(((v < q1 - 1.5 * iqr) | (v > q3 + 1.5 * iqr)).sum()),
            "outliers_iqr_pct": float(((v < q1 - 1.5 * iqr) | (v > q3 + 1.5 * iqr)).mean() * 100),
            "is_constant": bool(constant),
        })
    return pd.DataFrame(rows)
