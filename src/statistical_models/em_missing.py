"""Bài 2: EM cho phân phối chuẩn nhiều chiều N(µ, Σ) với dữ liệu cảm biến bị khuyết.

E-step: x̂_m = µ_m + Σ_mo Σ_oo⁻¹ (x_o − µ_o),   C_i[m, m] = Σ_mm − Σ_mo Σ_oo⁻¹ Σ_om
M-step: µ̂ = (1/n) Σ_i x̂_i,                    Σ̂ = (1/n) [Σ_i (x̂_i − µ̂)(x̂_i − µ̂)ᵀ + Σ_i C_i]

Log-likelihood dữ liệu quan sát được ℓ(θ) = Σ_i log N(x_i,o | µ_o, Σ_oo) không bao giờ giảm qua các vòng lặp.
Đánh giá: xóa ngẫu nhiên 10 % ô (MCAR), so RMSE của EM với điền trung bình trên đúng các ô đã xóa.
"""

from __future__ import annotations
from .data import split_by_group
from dataclasses import dataclass, field

import numpy as np
import pandas as pd

from . import config as cfg

_LOG2PI = np.log(2 * np.pi)

@dataclass
class EMResult:
    mu: np.ndarray
    sigma: np.ndarray
    filled: np.ndarray
    history: list[float] = field(default_factory=list)
    n_iter: int = 0
    converged: bool = False

def _patterns(miss: np.ndarray) -> dict[bytes, np.ndarray]:
    """Gom các dòng có cùng mẫu khuyết để mỗi vòng lặp chỉ nghịch đảo Σ_oo một lần cho mỗi mẫu."""
    keys = np.packbits(miss, axis=1)
    out: dict[bytes, list[int]] = {}
    for i, k in enumerate(map(bytes, keys)):
        out.setdefault(k, []).append(i)
    return {k: np.asarray(v) for k, v in out.items()}

def observed_loglik(X: np.ndarray, miss: np.ndarray, mu: np.ndarray, sigma: np.ndarray,
                    groups: dict | None = None) -> float:
    groups = _patterns(miss) if groups is None else groups
    ll = 0.0
    for rows in groups.values():
        o = ~miss[rows[0]]
        if not o.any():
            continue
        S = sigma[np.ix_(o, o)]
        L = np.linalg.cholesky(S)
        D = np.linalg.solve(L, (X[np.ix_(rows, o)] - mu[o]).T)
        logdet = 2 * np.sum(np.log(np.diag(L)))
        ll += -0.5 * (len(rows) * (o.sum() * _LOG2PI + logdet) + np.sum(D ** 2))
    return float(ll)

def em_mvn(X_nan: np.ndarray, max_iter: int = cfg.EM_MAX_ITER, tol: float = cfg.EM_TOL) -> EMResult:
    """Ước lượng MLE (µ, Σ) khi X có NaN và điền giá trị khuyết bằng kỳ vọng có điều kiện."""
    X = np.asarray(X_nan, float)
    n, d = X.shape
    miss = np.isnan(X)
    groups = _patterns(miss)
    # khởi tạo: trung bình và phương sai từng cột trên phần quan sát được, hiệp phương sai = 0
    mu = np.nanmean(X, axis=0)
    sigma = np.diag(np.nanvar(X, axis=0))
    filled = np.where(miss, mu, X)
    history = [observed_loglik(X, miss, mu, sigma, groups)]
    converged, it = False, 0
    for it in range(1, max_iter + 1):
        C = np.zeros((d, d))
        for rows in groups.values():
            m = miss[rows[0]]
            if not m.any():
                continue
            o = ~m
            if not o.any():  # cả dòng bị mất: kỳ vọng là µ, phương sai có điều kiện là Σ
                filled[rows] = mu
                C += len(rows) * sigma
                continue
            S_oo, S_mo = sigma[np.ix_(o, o)], sigma[np.ix_(m, o)]
            K = np.linalg.solve(S_oo, S_mo.T).T                     # Σ_mo Σ_oo⁻¹
            filled[np.ix_(rows, m)] = mu[m] + (X[np.ix_(rows, o)] - mu[o]) @ K.T
            C[np.ix_(m, m)] += len(rows) * (sigma[np.ix_(m, m)] - K @ S_mo.T)
        mu = filled.mean(axis=0)
        R = filled - mu
        sigma = (R.T @ R + C) / n
        sigma = (sigma + sigma.T) / 2
        ll = observed_loglik(X, miss, mu, sigma, groups)
        history.append(ll)
        if abs(ll - history[-2]) < tol * (abs(history[-2]) + 1):
            converged = True
            break
    return EMResult(mu, sigma, conditional_impute(X, mu, sigma), history, it, converged)

def conditional_impute(X_nan: np.ndarray, mu: np.ndarray, sigma: np.ndarray) -> np.ndarray:
    """Điền ô khuyết bằng kỳ vọng có điều kiện x̂_m = µ_m + Σ_mo Σ_oo⁻¹ (x_o − µ_o) với (µ, Σ) cho trước.

    Dùng để áp tham số EM đã học trên tập train lên dữ liệu mới (tập valid)."""
    X = np.asarray(X_nan, float)
    miss = np.isnan(X)
    out = X.copy()
    for rows in _patterns(miss).values():
        m = miss[rows[0]]
        if not m.any():
            continue
        o = ~m
        if not o.any():
            out[rows] = mu
            continue
        K = np.linalg.solve(sigma[np.ix_(o, o)], sigma[np.ix_(m, o)].T).T
        out[np.ix_(rows, m)] = mu[m] + (X[np.ix_(rows, o)] - mu[o]) @ K.T
    return out

def mean_impute(X_nan: np.ndarray) -> np.ndarray:
    X = np.asarray(X_nan, float)
    return np.where(np.isnan(X), np.nanmean(X, axis=0), X)

def mcar_mask(shape: tuple[int, int], rate: float, rng: np.random.Generator) -> np.ndarray:
    """Mặt nạ MCAR: mỗi ô bị xóa độc lập với xác suất `rate`; không xóa trọn một dòng."""
    m = rng.random(shape) < rate
    full = m.all(axis=1)
    while full.any():
        m[full] = rng.random((full.sum(), shape[1])) < rate
        full = m.all(axis=1)
    return m

def outage_mask(batch_keys: np.ndarray, n_cols: int, rng: np.random.Generator,
                n_outages: int = cfg.EM_OUTAGES_PER_BATCH, min_len: int = cfg.EM_OUTAGE_MIN,
                max_len: int = cfg.EM_OUTAGE_MAX) -> np.ndarray:
    """Kịch bản mất tín hiệu (theo kế hoạch TV6): trong mỗi lô, `n_outages` cảm biến khác nhau, mỗi cảm biến mất
    liền một đoạn 10–15 phút. Các dòng phải được sắp theo (batch_key, minute). Không bao giờ mất trọn một dòng."""
    keys = np.asarray(batch_keys)
    m = np.zeros((len(keys), n_cols), bool)
    for k in np.unique(keys):
        idx = np.flatnonzero(keys == k)
        for c in rng.choice(n_cols, size=min(n_outages, n_cols - 1), replace=False):
            L = int(rng.integers(min_len, max_len + 1))
            if len(idx) <= L:
                continue
            start = int(rng.integers(0, len(idx) - L + 1))
            m[idx[start:start + L], c] = True
    return m

def interpolate_in_batch(X_nan: np.ndarray, batch_keys: np.ndarray) -> np.ndarray:
    """Baseline theo thời gian: nội suy tuyến tính trong từng lô (đầu/cuối lô lấy giá trị gần nhất).
    Các dòng phải được sắp theo thời gian trong từng lô."""
    X = np.asarray(X_nan, float).copy()
    keys = np.asarray(batch_keys)
    for k in np.unique(keys):
        idx = np.flatnonzero(keys == k)
        block = pd.DataFrame(X[idx]).interpolate(limit_direction="both").to_numpy(float)
        X[idx] = block
    return X

def rmse_by_column(truth: np.ndarray, est: np.ndarray, mask: np.ndarray, cols: list[str]) -> pd.Series:
    def _r(sel_truth, sel_est):
        return float(np.sqrt(np.mean((sel_truth - sel_est) ** 2))) if len(sel_truth) else np.nan

    out = {c: _r(truth[mask[:, j], j], est[mask[:, j], j]) for j, c in enumerate(cols)}
    out["TOAN_BO"] = _r(truth[mask], est[mask])
    return pd.Series(out)

def _rmse_table(truth: np.ndarray, fills: dict, mask: np.ndarray, cols: list[str], split: str) -> pd.DataFrame:
    """fills: {tên phương pháp: ma trận đã điền}; luôn có "EM" và "trung_binh"."""
    t = pd.DataFrame({k: rmse_by_column(truth, v, mask, cols) for k, v in fills.items()})
    t["giam_rmse_pct"] = 100 * (1 - t["EM"] / t["trung_binh"])
    t.insert(0, "tap", split)
    t.insert(1, "so_o_bi_xoa", [int(mask[:, j].sum()) for j in range(len(cols))] + [int(mask.sum())])
    return t.rename_axis("bien").reset_index()

def run_em_experiment(minutes: pd.DataFrame, cols: tuple[str, ...] = cfg.SENSOR_VARS,
                      n_rows: int = cfg.EM_SAMPLE_ROWS, rate: float = cfg.EM_MISSING_RATE,
                      valid_fraction: float = cfg.EM_VALID_FRACTION, seed: int = cfg.SEED) -> dict:
    """Train/valid tách theo lô (batch_key):

    - Train: lấy mẫu n_rows phút từ các lô train, xóa `rate` ô (MCAR), EM học µ, Σ trên dữ liệu khuyết đó.
    - Valid (các lô EM chưa thấy), hai kịch bản, điền bằng µ, Σ đã học ở train:
        (a) valid_mcar: xóa ngẫu nhiên `rate` ô (theo "Bài toán ML");
        (b) valid_mat_tin_hieu: mỗi lô có 2 cảm biến mất liền 10–15 phút (theo kế hoạch TV6).
    - Baseline: điền trung bình (của tập train); ở tập valid thêm nội suy tuyến tính theo thời gian trong từng lô.
    z-score dùng trung bình/độ lệch chuẩn của phần quan sát được ở tập train (áp nguyên cho tập valid).
    RMSE chỉ tính trên các ô đã bị xóa, so với giá trị thật trước khi xóa.
    """

    rng = np.random.default_rng(seed)
    cols = list(cols)
    data = minutes.dropna(subset=cols).sort_values(["batch_key", "minute"]).reset_index(drop=True)
    is_tr, is_va = split_by_group(data["batch_key"].to_numpy(), valid_fraction, seed)
    train = data[is_tr].sample(n=min(n_rows, int(is_tr.sum())), random_state=seed).reset_index(drop=True)
    valid = data[is_va].reset_index(drop=True)                      # giữ thứ tự (batch_key, minute)

    raw_tr, raw_va = train[cols].to_numpy(float), valid[cols].to_numpy(float)
    mask_tr = mcar_mask(raw_tr.shape, rate, rng)
    obs_tr = np.where(mask_tr, np.nan, raw_tr)
    mean, sd = np.nanmean(obs_tr, axis=0), np.nanstd(obs_tr, axis=0)
    sd = np.where(sd > 0, sd, 1.0)
    Z_tr, Z_va = (raw_tr - mean) / sd, (raw_va - mean) / sd          # giá trị thật (để chấm điểm)
    Zn_tr = np.where(mask_tr, np.nan, Z_tr)

    em = em_mvn(Zn_tr)
    mean_tr = np.nanmean(Zn_tr, axis=0)                             # baseline: trung bình tập train
    fill_mean_tr = np.where(mask_tr, mean_tr, Zn_tr)
    tables = [_rmse_table(Z_tr, {"EM": em.filled, "trung_binh": fill_mean_tr}, mask_tr, cols, "train")]

    keys_va = valid["batch_key"].to_numpy()
    masks_va = {"valid_mcar": mcar_mask(raw_va.shape, rate, rng),
                "valid_mat_tin_hieu": outage_mask(keys_va, len(cols), rng)}
    filled_va = {}
    for name, mk in masks_va.items():
        Zn = np.where(mk, np.nan, Z_va)
        fills = {"EM": conditional_impute(Zn, em.mu, em.sigma), "trung_binh": np.where(mk, mean_tr, Zn),
                 "noi_suy_thoi_gian": interpolate_in_batch(Zn, keys_va)}
        filled_va[name] = fills["EM"]
        tables.append(_rmse_table(Z_va, fills, mk, cols, name))
    rmse_z = pd.concat(tables, ignore_index=True)

    unit = dict(zip(cols, sd))
    rmse_raw = rmse_z.copy()
    for c in ("EM", "trung_binh", "noi_suy_thoi_gian"):
        rmse_raw[c] = [v * unit[b] if b in unit else np.nan for b, v in zip(rmse_z["bien"], rmse_z[c])]

    sigma_full = np.cov(Z_tr, rowvar=False, ddof=0)
    return {
        "cols": cols, "train": train, "valid": valid, "mask_train": mask_tr, "masks_valid": masks_va,
        "Z_train": Z_tr, "Z_valid": Z_va, "z_mean": mean, "z_sd": sd, "em": em,
        "filled_valid": filled_va, "rmse_z": rmse_z, "rmse_raw": rmse_raw, "sigma_full_train": sigma_full,
        "split": {"n_batches_train": int(train["batch_key"].nunique()),
                  "n_batches_valid": int(valid["batch_key"].nunique()),
                  "n_rows_train": int(len(train)), "n_rows_valid": int(len(valid)),
                  "batch_overlap": int(len(set(train["batch_key"]) & set(valid["batch_key"])))},
        "param_error": {
            "max_abs_mu_em_vs_full_train": float(np.max(np.abs(em.mu - Z_tr.mean(0)))),
            "max_abs_sigma_em_vs_full_train": float(np.max(np.abs(em.sigma - sigma_full))),
            "max_abs_sigma_meanimpute_vs_full_train":
                float(np.max(np.abs(np.cov(fill_mean_tr, rowvar=False, ddof=0) - sigma_full))),
        },
        "missing_cells": {"train": int(mask_tr.sum()), **{k: int(v.sum()) for k, v in masks_va.items()}},
        "loglik_monotone": bool(np.all(np.diff(em.history) >= -1e-8)),
    }


def fit_final(minutes: pd.DataFrame, cols: tuple[str, ...] = cfg.SENSOR_VARS) -> dict:
    """Sau khi validate: học µ, Σ trên TOÀN BỘ dữ liệu phút để bàn giao.

    Gold hiện không có ô khuyết nên EM dừng ngay ở trung bình/hiệp phương sai mẫu (đúng là MLE của dữ liệu đầy đủ);
    nếu sau này Gold có ô khuyết thì hàm vẫn chạy EM đầy đủ."""
    cols = list(cols)
    X = minutes[cols].to_numpy(float)
    mean, sd = np.nanmean(X, axis=0), np.nanstd(X, axis=0)
    sd = np.where(sd > 0, sd, 1.0)
    em = em_mvn((X - mean) / sd)
    return {"cols": cols, "z_mean": mean, "z_sd": sd, "em": em, "n_rows": int(len(X)),
            "n_missing_cells": int(np.isnan(X).sum()),
            "mu": em.mu * sd + mean, "sigma": em.sigma * np.outer(sd, sd)}

def conditional_impute_sd(X_nan: np.ndarray, mu: np.ndarray, sigma: np.ndarray) -> tuple[np.ndarray, np.ndarray]:
    """Như conditional_impute, kèm độ lệch chuẩn có điều kiện của từng ô được điền (ô đo được có sd = 0).

    Var(x_m | x_o) = Σ_mm − Σ_mo Σ_oo⁻¹ Σ_om. Web vẽ dải ±1,96 sd quanh giá trị điền."""
    X = np.asarray(X_nan, float)
    miss = np.isnan(X)
    out, sd = X.copy(), np.zeros_like(X)
    for rows in _patterns(miss).values():
        m = miss[rows[0]]
        if not m.any():
            continue
        o = ~m
        if not o.any():
            out[rows], sd[rows] = mu, np.sqrt(np.diag(sigma))
            continue
        K = np.linalg.solve(sigma[np.ix_(o, o)], sigma[np.ix_(m, o)].T).T
        out[np.ix_(rows, m)] = mu[m] + (X[np.ix_(rows, o)] - mu[o]) @ K.T
        cond = sigma[np.ix_(m, m)] - K @ sigma[np.ix_(o, m)]
        sd[np.ix_(rows, m)] = np.sqrt(np.clip(np.diag(cond), 0, None))
    return out, sd

def band_coverage(truth: np.ndarray, X_nan: np.ndarray, mu: np.ndarray, sigma: np.ndarray, z: float = 1.96) -> float:
    """Tỉ lệ ô bị xóa có giá trị thật nằm trong dải điền ±z·sd (dải 95 % đúng thì phải gần 0,95)."""
    filled, sd = conditional_impute_sd(X_nan, mu, sigma)
    m = np.isnan(np.asarray(X_nan, float))
    return float(np.mean(np.abs(truth[m] - filled[m]) <= z * sd[m]))

def choose_fill_method(rmse_z: pd.DataFrame, scenario: str = "valid_mat_tin_hieu") -> dict:
    """Cách điền cho từng cảm biến, theo RMSE ở kịch bản mất tín hiệu trên các lô chưa thấy.
    Đang mất (chưa có số đo sau đoạn mất): chỉ EM dùng được, vì nội suy cần điểm ở cả hai đầu.
    Sau khi có tín hiệu lại: chọn EM hoặc nội suy trong lô, cách nào sai số nhỏ hơn."""
    t = rmse_z[(rmse_z["tap"] == scenario) & (rmse_z["bien"] != "TOAN_BO")]
    out = {}
    for r in t.itertuples():
        after = "noi_suy" if r.noi_suy_thoi_gian < r.EM else "EM"
        out[r.bien] = {"khi_dang_mat": "EM", "sau_khi_co_tin_hieu_lai": after,
                       "rmse_z_EM": float(r.EM), "rmse_z_noi_suy": float(r.noi_suy_thoi_gian),
                       "rmse_z_trung_binh": float(r.trung_binh)}
    return out

def outage_action(sensor: str, minutes_missing: int, hold_sensor: str = cfg.EM_HOLD_SENSOR,
                  max_minutes: int = cfg.EM_HOLD_MAX_MINUTES) -> dict:
    """Quy tắc nghiệp vụ: mất nhiệt UHT quá lâu thì giữ lô; cảm biến khác thì điền và đánh dấu lô."""
    if sensor == hold_sensor and minutes_missing > max_minutes:
        return {"action": "GIU_LO",
                "message": f"Mất {sensor} {minutes_missing} phút (quá {max_minutes} phút): giữ lô, bắt buộc kiểm vi sinh. "
                           "Không dùng số ước lượng để cho xuất lô."}
    return {"action": "DIEN_VA_DANH_DAU",
            "message": f"Điền {sensor} bằng ước lượng và đánh dấu lô có dữ liệu được điền. Vẫn chấm điểm rủi ro, độ tin cậy giảm."}