"""Sàng lọc biến cho TV6: tương quan (p-value đã hiệu chỉnh), VIF, loại biến hằng số (TV5)."""

from __future__ import annotations

import numpy as np
import pandas as pd
from scipy import stats

from .config import NON_FEATURE_COLUMNS, TARGETS, VIF_THRESHOLD


def candidate_columns(df: pd.DataFrame) -> list[str]:
    """Các cột số có thể làm đặc trưng, đã bỏ khóa/định danh và nhãn mục tiêu."""
    skip = set(NON_FEATURE_COLUMNS) | set(TARGETS)
    return [c for c in df.select_dtypes(include=[np.number, "bool"]).columns if c not in skip]


def benjamini_hochberg(p: np.ndarray) -> np.ndarray:
    """Hiệu chỉnh FDR kiểu Benjamini–Hochberg. Trả về q-value cùng thứ tự với p đầu vào."""
    p = np.asarray(p, float)
    order = np.argsort(p)
    ranked = p[order] * len(p) / (np.arange(len(p)) + 1)
    q = np.minimum.accumulate(ranked[::-1])[::-1]  # đảm bảo q không giảm khi p tăng
    out = np.empty_like(q)
    out[order] = np.clip(q, 0, 1)
    return out


def correlation_matrix(df: pd.DataFrame, cols: list[str], method: str = "spearman") -> tuple[pd.DataFrame, pd.DataFrame]:
    """Ma trận tương quan và ma trận p-value tương ứng."""
    d = df[cols].astype(float)
    corr = d.corr(method=method)
    pv = pd.DataFrame(np.ones_like(corr.values), index=cols, columns=cols)
    for i, a in enumerate(cols):
        for b in cols[i + 1:]:  # chỉ tính nửa tam giác trên rồi lấy đối xứng
            m = d[[a, b]].dropna()
            if len(m) > 3 and m[a].nunique() > 1 and m[b].nunique() > 1:
                f = stats.spearmanr if method == "spearman" else stats.pearsonr
                pv.loc[a, b] = pv.loc[b, a] = float(f(m[a], m[b])[1])
    return corr, pv


def target_correlations(df: pd.DataFrame, cols: list[str], targets=TARGETS) -> pd.DataFrame:
    """Tương quan của từng đặc trưng với biến mục tiêu, kèm q-value BH."""
    rows = []
    for t in targets:
        if t not in df.columns:
            continue
        for c in cols:
            m = df[[c, t]].dropna().astype(float)
            if len(m) < 10 or m[c].nunique() < 2 or m[t].nunique() < 2:
                continue  # quá ít dữ liệu hoặc một cột không đổi thì không tính được
            rho, p = stats.spearmanr(m[c], m[t])
            r, _ = stats.pearsonr(m[c], m[t])
            rows.append({"target": t, "feature": c, "n": len(m), "spearman": rho, "pearson": r, "p_value": p})
    out = pd.DataFrame(rows)
    if len(out):
        # chạy nhiều phép kiểm định cùng lúc nên phải hiệu chỉnh, không thì dễ "tìm ra" tương quan giả
        out["q_value_bh"] = benjamini_hochberg(out["p_value"].to_numpy())
        out["significant_q_lt_0_05"] = out["q_value_bh"] < 0.05
    return out


def vif(df: pd.DataFrame, cols: list[str]) -> pd.Series:
    """VIF_j = 1/(1−R²_j), với R²_j từ hồi quy cột j theo các cột còn lại (giải bằng numpy lstsq)."""
    X = df[cols].astype(float).dropna()
    Z = (X - X.mean()) / X.std(ddof=0)  # chuẩn hóa để hệ số hồi quy không phụ thuộc đơn vị
    out = {}
    for j, c in enumerate(cols):
        y = Z[c].to_numpy()
        A = np.column_stack([np.ones(len(Z)), Z.drop(columns=c).to_numpy()]) if len(cols) > 1 else np.ones((len(Z), 1))
        beta, *_ = np.linalg.lstsq(A, y, rcond=None)
        resid = y - A @ beta
        r2 = 1 - resid.var() / y.var()
        out[c] = np.inf if r2 >= 1 - 1e-12 else 1 / (1 - r2)  # R² = 1 nghĩa là cột này là tổ hợp của các cột khác
    return pd.Series(out)


def screen_features(df: pd.DataFrame, threshold: float = VIF_THRESHOLD) -> dict:
    """Quy trình: bỏ biến hằng số/thiếu nhiều, rồi loại dần biến có VIF cao nhất. Ghi lại lý do từng biến bị loại."""
    cols = candidate_columns(df)
    dropped: list[dict] = []

    for c in list(cols):
        if df[c].nunique(dropna=True) <= 1:
            cols.remove(c)
            dropped.append({"feature": c, "reason": "hằng số (phương sai bằng 0)"})
        elif df[c].isna().mean() > 0.3:
            cols.remove(c)
            dropped.append({"feature": c, "reason": f"thiếu {df[c].isna().mean():.0%} giá trị"})

    d = df[cols].dropna()
    vif_initial = vif(d, cols) if len(cols) > 1 else pd.Series({c: 1.0 for c in cols})
    # mỗi vòng chỉ loại 1 biến (VIF cao nhất) rồi tính lại, vì bỏ một biến sẽ kéo VIF của các biến còn lại xuống
    while len(cols) > 1:
        v = vif(d, cols)
        worst = v.idxmax()
        if v[worst] <= threshold:
            break
        dropped.append({"feature": worst, "reason": f"VIF = {v[worst]:.1f} > {threshold}"})
        cols.remove(worst)
        d = d[cols]

    final_vif = vif(d, cols) if len(cols) > 1 else pd.Series({c: 1.0 for c in cols})
    return {
        "selected_features": cols,
        "dropped": dropped,
        "vif_final": {k: round(float(v), 3) for k, v in final_vif.items()},
        "vif_initial": {k: round(float(min(v, 1e6)), 3) for k, v in vif_initial.items()},  # chặn inf cho JSON
        "vif_threshold": threshold,
        "excluded_as_ids_or_targets": sorted(set(NON_FEATURE_COLUMNS) | set(TARGETS)),
    }
