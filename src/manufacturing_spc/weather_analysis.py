"""Phân tích ảnh hưởng thời tiết (chỉ số THI) đến chất lượng sữa (TV5)."""

from __future__ import annotations

import numpy as np
import pandas as pd
from scipy import stats

from .config import THI_QUALITY_VARS

THI_THRESHOLD = 72  # THI > 72 thì bò sữa bị stress nhiệt (kế hoạch, mục 2.3)


def ensure_heat_stress(batches: pd.DataFrame) -> pd.DataFrame:
    """Đảm bảo có cột is_heat_stress; nếu thiếu thì suy ra từ thi_index > 72."""
    if "is_heat_stress" not in batches.columns and "thi_index" in batches.columns:
        batches = batches.assign(is_heat_stress=(batches["thi_index"] > THI_THRESHOLD).astype(int))
    return batches


def thi_correlations(batches: pd.DataFrame) -> pd.DataFrame:
    """Tương quan Pearson và Spearman giữa thi_index và fat_in, protein_in, yield_recovery_pct."""
    rows = []
    for q in THI_QUALITY_VARS:
        if q not in batches.columns or "thi_index" not in batches.columns:
            continue
        m = batches[["thi_index", q]].dropna().astype(float)
        if len(m) < 10 or m["thi_index"].nunique() < 2 or m[q].nunique() < 2:
            continue
        r, pr = stats.pearsonr(m["thi_index"], m[q])
        rho, ps = stats.spearmanr(m["thi_index"], m[q])
        rows.append({"variable": q, "n": len(m), "pearson_r": r, "pearson_p": pr,
                     "spearman_rho": rho, "spearman_p": ps})
    return pd.DataFrame(rows)


def heat_stress_tests(batches: pd.DataFrame) -> pd.DataFrame:
    """So sánh hai nhóm is_heat_stress ∈ {0,1}: Mann–Whitney cho biến liên tục, Fisher exact cho cờ lỗi lô."""
    b = ensure_heat_stress(batches)
    if "is_heat_stress" not in b.columns:
        return pd.DataFrame()
    g0, g1 = b[b["is_heat_stress"] == 0], b[b["is_heat_stress"] == 1]
    rows = []
    for q in THI_QUALITY_VARS:
        if q not in b.columns:
            continue
        a, c = g0[q].dropna().to_numpy(float), g1[q].dropna().to_numpy(float)
        if len(a) < 3 or len(c) < 3:
            rows.append({"test": "Mann-Whitney U", "variable": q, "n_group0": len(a), "n_group1": len(c),
                         "note": "nhóm quá nhỏ (<3), không kiểm định được"})
            continue
        u, p = stats.mannwhitneyu(c, a, alternative="two-sided")  # U của nhóm stress nhiệt so với nhóm bình thường
        rows.append({"test": "Mann-Whitney U", "variable": q, "n_group0": len(a), "n_group1": len(c),
                     "median_group0": float(np.median(a)), "median_group1": float(np.median(c)),
                     "statistic": float(u), "p_value": float(p),
                     "effect_rank_biserial": float(2 * u / (len(a) * len(c)) - 1)})
    if "batch_defect_flag" in b.columns:
        t = pd.crosstab(b["is_heat_stress"], b["batch_defect_flag"]).reindex(index=[0, 1], columns=[0, 1], fill_value=0)
        odds, p = stats.fisher_exact(t.to_numpy())
        rows.append({"test": "Fisher exact", "variable": "batch_defect_flag", "n_group0": int(t.loc[0].sum()),
                     "n_group1": int(t.loc[1].sum()), "statistic": float(odds), "p_value": float(p),
                     "note": f"bảng 2x2 [không stress: {t.loc[0, 0]}/{t.loc[0, 1]}, stress: {t.loc[1, 0]}/{t.loc[1, 1]}] (không lỗi/lỗi); statistic là odds ratio"})
    return pd.DataFrame(rows)
