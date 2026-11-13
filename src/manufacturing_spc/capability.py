"""Chỉ số năng lực quy trình Cp, Cpk, Cpm, Pp, Ppk (TV5)."""

from __future__ import annotations

from math import sqrt

import numpy as np
import pandas as pd
from scipy import stats

from .config import CAPABLE_CPK, Spec


def capability(x: np.ndarray, spec: Spec, sigma_within: float | None = None) -> dict:
    """Tính năng lực quy trình.

    - Cp/Cpk dùng sigma_within (S̄/c4 từ nhóm con): độ biến thiên ngắn hạn, cho biết tiềm năng.
    - Pp/Ppk dùng sigma tổng thể (độ lệch chuẩn toàn bộ dữ liệu): phản ánh thực tế dài hạn.
    - Cpm phạt thêm khi giá trị trung bình lệch khỏi mục tiêu T.
    Giả định phân phối gần chuẩn và quy trình đã ổn định (qua Phase I), nên đọc kèm normality_p.
    """
    x = np.asarray(x, float)
    x = x[~np.isnan(x)]
    n, mu = len(x), float(x.mean())
    s_overall = float(x.std(ddof=1))
    s_w = float(sigma_within) if sigma_within else s_overall  # không có sigma_within thì dùng tạm sigma tổng
    usl, lsl, t = spec.usl, spec.lsl, spec.mid

    def indices(sig: float) -> tuple[float, float]:
        # Cp: bề rộng spec so với 6 sigma; Cpk: khoảng cách đến biên gần nhất tính theo 3 sigma
        return (usl - lsl) / (6 * sig), min((usl - mu) / (3 * sig), (mu - lsl) / (3 * sig))

    cp, cpk = indices(s_w)
    pp, ppk = indices(s_overall)
    cpm = (usl - lsl) / (6 * sqrt(s_overall**2 + (mu - t) ** 2))

    # khoảng tin cậy 95% xấp xỉ cho Cpk (công thức Bissell)
    se = sqrt(1 / (9 * n) + cpk**2 / (2 * (n - 1)))
    ppm_low = stats.norm.cdf((lsl - mu) / s_w) * 1e6
    ppm_high = stats.norm.sf((usl - mu) / s_w) * 1e6

    # kiểm định chuẩn trên mẫu tối đa 5000 điểm cho nhanh và đỡ "quá nhạy" với dữ liệu khổng lồ
    sample = x if n <= 5000 else np.random.default_rng(0).choice(x, 5000, replace=False)
    normality_p = float(stats.normaltest(sample).pvalue)

    return {
        "variable": spec.name, "n": n, "mean": mu, "sigma_within": s_w, "sigma_overall": s_overall,
        "LSL": lsl, "USL": usl, "target": t,
        "Cp": cp, "Cpk": cpk, "Cpm": cpm, "Pp": pp, "Ppk": ppk,
        "Cpk_ci95_low": cpk - 1.96 * se, "Cpk_ci95_high": cpk + 1.96 * se,
        "ppm_below_LSL": ppm_low, "ppm_above_USL": ppm_high,
        "observed_ppm_out_of_spec": float(((x < lsl) | (x > usl)).mean() * 1e6),
        "normality_p": normality_p,
        "capable_cpk_ge_1_33": bool(cpk >= CAPABLE_CPK),
        "conclusion": (f"Đạt (Cpk = {cpk:.2f} ≥ {CAPABLE_CPK})" if cpk >= CAPABLE_CPK
                       else f"Không đạt (Cpk = {cpk:.2f} < {CAPABLE_CPK})"),
    }


def capability_table(rows: list[dict]) -> pd.DataFrame:
    return pd.DataFrame(rows)
