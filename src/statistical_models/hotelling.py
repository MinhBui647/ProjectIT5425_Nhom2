"""Giám sát dây chuyền / lô cần kiểm tra bằng Hotelling T².

T² = (x − µ)ᵀ Σ⁻¹ (x − µ), x: trung bình 1 phút của 7 cảm biến. Ổn định thì T² ~ χ²(p).
Thiếu cảm biến thì chỉ tính trên các chiều còn đo được, so với χ²(số chiều còn lại).
Đóng góp c_j = (x_j − µ_j) · [Σ⁻¹ (x − µ)]_j, tổng các c_j bằng T².
Train: µ, Σ học bằng EM trên Phase I (tháng 01). Validate: Phase II (tháng 02–06).
"""
from __future__ import annotations

import numpy as np
import pandas as pd
from scipy import stats
from . import config as cfg

def t2_scores(X: np.ndarray, mu: np.ndarray, sigma: np.ndarray,
              alpha: float = cfg.T2_ALPHA) -> tuple[pd.DataFrame, np.ndarray]:
    """T², số chiều quan sát, ngưỡng χ² và đóng góp từng biến cho mỗi dòng (bỏ qua NaN)."""
    X = np.asarray(X, float)
    n, p = X.shape
    t2 = np.full(n, np.nan)
    dof = (~np.isnan(X)).sum(1)
    contrib = np.full((n, p), np.nan)
    miss = np.isnan(X)
    for pattern in np.unique(miss, axis=0):
        rows = np.flatnonzero((miss == pattern).all(1))
        o = ~pattern
        if not o.any():
            continue
        inv = np.linalg.inv(sigma[np.ix_(o, o)])
        dev = X[np.ix_(rows, np.flatnonzero(o))] - mu[o]
        w = dev @ inv
        c = dev * w
        contrib[np.ix_(rows, np.flatnonzero(o))] = c
        t2[rows] = c.sum(1)
    limit = np.where(dof > 0, stats.chi2.ppf(1 - alpha, np.maximum(dof, 1)), np.nan)
    return pd.DataFrame({"t2": t2, "n_observed": dof, "limit": limit, "signal": t2 > limit}), contrib

def run_hotelling(minutes: pd.DataFrame, mu: np.ndarray, sigma: np.ndarray, cols: list[str],
                  batches: pd.DataFrame | None = None, alpha: float = cfg.T2_ALPHA) -> dict:
    scores, contrib = t2_scores(minutes[cols].to_numpy(float), np.asarray(mu, float), np.asarray(sigma, float), alpha)
    out = pd.concat([minutes[["batch_key", "minute"]].reset_index(drop=True), scores], axis=1)
    top = np.nanargmax(np.where(np.isnan(contrib), -np.inf, contrib), axis=1)
    out["top_sensor"] = [cols[i] for i in top]
    out["top_share"] = np.nanmax(contrib, axis=1) / out["t2"]
    if batches is not None and {"batch_key", "batch_id"} <= set(batches.columns):
        extra = [c for c in ("batch_id", "line_id") if c in batches.columns]
        out = out.merge(batches[["batch_key", *extra]].drop_duplicates("batch_key"), on="batch_key", how="left")
    per_batch = (out.groupby("batch_key").agg(n_minutes=("t2", "size"), n_signals=("signal", "sum"),
                                              t2_max=("t2", "max")).reset_index())
    rate = float(out["signal"].mean())
    return {
        "alpha": alpha, "p": len(cols), "limit_full": float(stats.chi2.ppf(1 - alpha, len(cols))),
        "n_minutes": int(len(out)), "n_signals": int(out["signal"].sum()), "signal_rate": rate,
        "expected_rate": alpha,
        # cảm biến góp nhiều nhất ở các phút bị báo
        "top_sensor_counts": out.loc[out["signal"], "top_sensor"].value_counts().to_dict(),
        "minutes": out, "per_batch": per_batch,
    }

def detection_check(mu: np.ndarray, sigma: np.ndarray, cols: list[str], n: int = 20000, shift_sd: float = 2.0,
                    alpha: float = cfg.T2_ALPHA, seed: int = cfg.SEED) -> dict:
    """Mô phỏng từ N(µ, Σ): báo động giả ≈ α; tổ hợp lạ (2 cảm biến tương quan dương mạnh nhất
    lệch ngược chiều nhau shift_sd độ lệch chuẩn) phải bị bắt."""
    rng = np.random.default_rng(seed)
    mu, sigma = np.asarray(mu, float), np.asarray(sigma, float)
    X = rng.multivariate_normal(mu, sigma, n)
    base = t2_scores(X, mu, sigma, alpha)[0]["signal"].mean()
    sd = np.sqrt(np.diag(sigma))
    corr = sigma / np.outer(sd, sd)
    np.fill_diagonal(corr, -np.inf)
    i, j = np.unravel_index(np.argmax(corr), corr.shape)
    Xs = X.copy()
    Xs[:, i] += shift_sd * sd[i]
    Xs[:, j] -= shift_sd * sd[j]
    univ = float((np.abs((Xs[:, [i, j]] - mu[[i, j]]) / sd[[i, j]]) > 3).any(1).mean())
    hit = t2_scores(Xs, mu, sigma, alpha)[0]["signal"].mean()
    return {"false_alarm_rate": float(base), "expected": alpha,
            "scenario": f"{cols[i]} +{shift_sd:g} sd và {cols[j]} −{shift_sd:g} sd (tương quan {corr[i, j]:.2f})",
            "t2_detection_rate": float(hit), "univariate_3sigma_detection_rate": univ}

def fit_phase1(minutes: pd.DataFrame, cols: list[str]) -> dict:
    """µ, Σ (đơn vị gốc) học bằng EM trên dữ liệu Phase I."""
    from .em_missing import em_mvn

    X = minutes[cols].to_numpy(float)
    mean, sd = np.nanmean(X, axis=0), np.nanstd(X, axis=0)
    sd = np.where(sd > 0, sd, 1.0)
    em = em_mvn((X - mean) / sd)
    return {"mu": em.mu * sd + mean, "sigma": em.sigma * np.outer(sd, sd), "n_iter": em.n_iter}

def univariate_signal(X: np.ndarray, mu: np.ndarray, sigma: np.ndarray, z: float = 3.0) -> np.ndarray:
    """Quy tắc so sánh: có ít nhất 1 cảm biến lệch quá z sd (NaN không tính)."""
    sd = np.sqrt(np.diag(sigma))
    with np.errstate(invalid="ignore"):
        return np.nan_to_num(np.abs((X - mu) / sd), nan=0.0).max(1) > z

def choose_k(n_signals: pd.Series, max_rate: float = cfg.T2_BATCH_MAX_FLAG_RATE) -> int:
    """k nhỏ nhất để tỉ lệ lô có ≥ k phút báo động không quá max_rate."""
    k = 1
    while (n_signals >= k).mean() > max_rate:
        k += 1
    return k

def inject_fault(X: np.ndarray, batch_keys: np.ndarray, shifts: dict[int, float], sd: np.ndarray,
                 length: int, rng: np.random.Generator) -> tuple[np.ndarray, np.ndarray]:
    """Mỗi lô có một đoạn liền `length` phút, cột j lệch shifts[j] · sd[j]. Trả về (X lỗi, mặt nạ phút lỗi)."""
    Xf, fault = X.copy(), np.zeros(len(X), bool)
    for key in np.unique(batch_keys):
        idx = np.flatnonzero(batch_keys == key)
        L = min(length, len(idx))
        start = rng.integers(0, len(idx) - L + 1)
        rows = idx[start:start + L]
        fault[rows] = True
        for j, s in shifts.items():
            Xf[rows, j] += s * sd[j]
    return Xf, fault

def _auc(y: np.ndarray, score: np.ndarray) -> float:
    from sklearn.metrics import roc_auc_score

    y = np.asarray(y)
    return float(roc_auc_score(y, score)) if 0 < y.sum() < len(y) else float("nan")

def _batch_counts(keys: np.ndarray, signal: np.ndarray) -> pd.Series:
    return pd.Series(signal.astype(int)).groupby(keys).sum()

def run_monitoring(minutes: pd.DataFrame, batches: pd.DataFrame, cols: tuple[str, ...] = cfg.SENSOR_VARS,
                   phase1_months: tuple[int, ...] = cfg.T2_PHASE1_MONTHS, alpha: float = cfg.T2_ALPHA,
                   max_rate: float = cfg.T2_BATCH_MAX_FLAG_RATE, shift_sd: float = cfg.T2_FAULT_SHIFT_SD,
                   fault_minutes: int = cfg.T2_FAULT_MINUTES, seed: int = cfg.SEED) -> dict:
    """Train Phase I (lô đạt): µ, Σ và k. Validate Phase II: báo động giả, mất cảm biến, lỗi giả lập, cờ lỗi lô."""
    from .em_missing import outage_mask

    rng = np.random.default_rng(seed)
    cols = list(cols)
    target = cfg.DEFECT_TARGET
    info = [c for c in ("batch_id", "line_id", "full_date") if c in batches.columns]
    b = batches[["batch_key", "date_key", target, *info]].drop_duplicates("batch_key")
    b = b.assign(month=(pd.to_numeric(b["date_key"]) // 100 % 100).astype(int))
    m = (minutes.dropna(subset=cols).merge(b[["batch_key", "month", target]], on="batch_key")
         .sort_values(["batch_key", "minute"]).reset_index(drop=True))
    in_p1 = m["month"].isin(phase1_months).to_numpy()
    p1 = m[in_p1 & (m[target] == 0).to_numpy()].reset_index(drop=True)
    p2 = m[~in_p1].reset_index(drop=True)
    if p1.empty or p2.empty:
        raise ValueError("Cần dữ liệu ở cả Phase I và Phase II (kiểm tra T2_PHASE1_MONTHS và date_key).")

    # train
    fit = fit_phase1(p1, cols)
    mu, sigma = fit["mu"], fit["sigma"]
    sd = np.sqrt(np.diag(sigma))
    X1, X2 = p1[cols].to_numpy(float), p2[cols].to_numpy(float)
    k1, k2 = p1["batch_key"].to_numpy(), p2["batch_key"].to_numpy()
    s1_t2 = t2_scores(X1, mu, sigma, alpha)[0]["signal"].to_numpy()
    s1_u = univariate_signal(X1, mu, sigma)
    k_t2, k_u = choose_k(_batch_counts(k1, s1_t2), max_rate), choose_k(_batch_counts(k1, s1_u), max_rate)

    # validate: báo động giả theo phút và theo lô
    h = run_hotelling(p2, mu, sigma, cols, batches, alpha)
    sig2 = h["minutes"]["signal"].to_numpy()
    u2 = univariate_signal(X2, mu, sigma)
    ok_rows = (p2[target] == 0).to_numpy()
    y_b = p2.groupby("batch_key")[target].first()
    ok_b = (y_b == 0).to_numpy()
    n_t2, n_u = _batch_counts(k2, sig2), _batch_counts(k2, u2)
    sets = (("Phase I (train), lô đạt", s1_t2, s1_u, np.ones(len(X1), bool)),
            ("Phase II, lô đạt", sig2, u2, ok_rows), ("Phase II, lô lỗi", sig2, u2, ~ok_rows))
    minute_level = pd.DataFrame([
        {"phuong_phap": name, "tap": tap, "so_phut": int(mask.sum()),
         "ti_le_bao_dong": float(s[mask].mean()) if mask.any() else np.nan}
        for tap, st, su, mask in sets for name, s in (("Hotelling T²", st), ("3 sigma từng cảm biến", su))])
    minute_level["ky_vong_alpha"] = np.where(minute_level["phuong_phap"] == "Hotelling T²", alpha, np.nan)
    batch_level = pd.DataFrame([
        {"phuong_phap": name, "k": k, "ti_le_gan_co_lo_dat": float((n[ok_b] >= k).mean()),
         "ti_le_gan_co_lo_loi": float((n[~ok_b] >= k).mean()) if (~ok_b).any() else np.nan,
         "auc_so_phut_bao_dong_vs_co_loi": _auc(y_b.to_numpy(), n.to_numpy())}
        for name, n, k in (("Hotelling T²", n_t2, k_t2), ("3 sigma từng cảm biến", n_u, k_u))])

    # validate: mất cảm biến (2 cảm biến mỗi lô mất 10–15 phút), báo động giả vẫn phải ≈ α
    mk = outage_mask(k2[ok_rows], len(cols), rng)
    so = t2_scores(np.where(mk, np.nan, X2[ok_rows]), mu, sigma, alpha)[0]["signal"]
    part = mk.any(1)
    outage = {"so_phut_thieu_cam_bien": int(part.sum()),
              "ti_le_bao_dong_phut_thieu": float(so[part].mean()) if part.any() else float("nan"),
              "ti_le_bao_dong_toan_bo": float(so.mean()), "ky_vong": alpha}

    # validate: chèn lỗi giả lập vào các lô đạt
    corr = sigma / np.outer(sd, sd)
    c = corr.copy()
    np.fill_diagonal(c, -np.inf)
    i, j = np.unravel_index(np.argmax(c), c.shape)
    u = cols.index(cfg.EM_HOLD_SENSOR) if cfg.EM_HOLD_SENSOR in cols else 0
    s = f"{shift_sd:g} sd"
    scenarios = {  # tên: (nhãn trên hình, độ lệch)
        f"{cols[u]} −{s} (lệch 1 cảm biến)": (f"Lệch 1 cảm biến\n({cols[u]} −{s})", {u: -shift_sd}),
        f"{cols[i]} +{s}, {cols[j]} −{s} (tổ hợp lạ, tương quan {corr[i, j]:.2f})":
            (f"Tổ hợp lạ\n({cols[i]} +{s},\n{cols[j]} −{s})", {i: shift_sd, j: -shift_sd}),
    }
    keys_ok, X_ok = k2[ok_rows], X2[ok_rows]
    rows = []
    for name, (label, shifts) in scenarios.items():
        Xf, fault = inject_fault(X_ok, keys_ok, shifts, sd, fault_minutes, rng)
        for meth, sig, k in (("Hotelling T²", t2_scores(Xf, mu, sigma, alpha)[0]["signal"].to_numpy(), k_t2),
                             ("3 sigma từng cảm biến", univariate_signal(Xf, mu, sigma), k_u)):
            rows.append({"kich_ban": name, "nhan": label, "phuong_phap": meth, "k": k,
                         "ti_le_bat_phut_loi": float(sig[fault].mean()),
                         "ti_le_bat_lo_loi": float((_batch_counts(keys_ok, sig) >= k).mean()),
                         "so_lo": int(len(np.unique(keys_ok)))})
    faults = pd.DataFrame(rows)

    # danh sách lô cần kiểm tra
    hm = h["minutes"]
    top = hm[hm["signal"]].groupby("batch_key")["top_sensor"].agg(lambda x: x.value_counts().index[0])
    lst = (h["per_batch"].set_index("batch_key").join(hm.groupby("batch_key")["t2"].mean().rename("t2_mean"))
           .join(top.rename("cam_bien_gop_nhieu_nhat")).join(b.set_index("batch_key")[[*info, target]])
           .reset_index())
    lst["can_kiem_tra"] = lst["n_signals"] >= k_t2
    lst["ly_do"] = np.where(lst["can_kiem_tra"], "T² vượt ngưỡng " + lst["n_signals"].astype(int).astype(str)
                            + " phút, chủ yếu do " + lst["cam_bien_gop_nhieu_nhat"].fillna("?"), "")
    lst = lst.sort_values(["can_kiem_tra", "n_signals", "t2_max"], ascending=False).reset_index(drop=True)

    return {
        "cols": cols, "mu": mu, "sigma": sigma, "alpha": alpha, "limit_full": h["limit_full"],
        "k_t2": k_t2, "k_univariate": k_u,
        "phase1": {"months": list(phase1_months), "n_batches": int(p1["batch_key"].nunique()),
                   "n_minutes": int(len(p1)),
                   "n_defect_batches_excluded": int(m.loc[in_p1 & (m[target] == 1).to_numpy(), "batch_key"].nunique()),
                   "em_n_iter": fit["n_iter"], "ti_le_lo_gan_co": float((_batch_counts(k1, s1_t2) >= k_t2).mean())},
        "phase2": {"n_batches": int(p2["batch_key"].nunique()), "n_minutes": int(len(p2)),
                   "n_defect_batches": int((~ok_b).sum()), "n_signals": h["n_signals"],
                   "top_sensor_counts": h["top_sensor_counts"], "n_lo_can_kiem_tra": int(lst["can_kiem_tra"].sum())},
        "minute_level": minute_level, "batch_level": batch_level, "outage": outage, "faults": faults,
        "inspection_list": lst, "t2_phase2": hm["t2"].to_numpy(),
        "simulation": detection_check(mu, sigma, cols, shift_sd=shift_sd, alpha=alpha, seed=seed),
    }