"""Bài 1: dự báo tỷ lệ thu hồi yield_recovery_pct.

- OLS (statsmodels) trên các lô độc lập (đã khử trùng, ~63 lô): hệ số, KTC 95 %, R².
- OLS trên toàn bộ 240 lô với sai số chuẩn cluster theo group_id (kiểm tra độ nhạy).
- ElasticNet (sklearn), chọn alpha và l1_ratio bằng GroupKFold; R² kiểm định chéo lồng nhau
  (vòng ngoài GroupKFold để chấm điểm, vòng trong GroupKFold để chọn siêu tham số).
- Chẩn đoán phần dư: Jarque–Bera, Shapiro–Wilk, Breusch–Pagan, Durbin–Watson.
"""

from __future__ import annotations

import inspect
from dataclasses import dataclass

import numpy as np
import pandas as pd
import statsmodels.api as sm
from scipy import stats
from sklearn.linear_model import ElasticNet, ElasticNetCV, LinearRegression, enet_path
from sklearn.model_selection import GroupKFold, KFold
from statsmodels.stats.diagnostic import het_breuschpagan
from statsmodels.stats.stattools import durbin_watson

from . import config as cfg
from .data import group_folds

def _n_alphas_kw(fn) -> dict:
    """sklearn >= 1.7 dùng alphas=<số nguyên>, bản cũ hơn dùng n_alphas (nhóm yêu cầu scikit-learn >= 1.4)."""
    params = inspect.signature(fn).parameters
    if "n_alphas" in params and params["n_alphas"].default != "deprecated":
        return {"n_alphas": cfg.EN_N_ALPHAS}
    return {"alphas": cfg.EN_N_ALPHAS}

def _design(df: pd.DataFrame, features: list[str], target: str) -> tuple[np.ndarray, np.ndarray]:
    return df[features].to_numpy(float), df[target].to_numpy(float)

def coef_table(res, model: str) -> pd.DataFrame:
    """Bảng hệ số statsmodels: hệ số, sai số chuẩn, thống kê, p-value, KTC 95 %."""
    ci = np.asarray(res.conf_int(0.05))
    return pd.DataFrame({
        "model": model, "term": res.model.exog_names,
        "coef": np.asarray(res.params), "std_err": np.asarray(res.bse),
        "stat": np.asarray(res.tvalues), "p_value": np.asarray(res.pvalues),
        "ci_low": ci[:, 0], "ci_high": ci[:, 1], "cov_type": res.cov_type,
    })

def fit_ols(df: pd.DataFrame, features: list[str], target: str = cfg.YIELD_TARGET,
            cluster: str | None = None):
    """OLS; nếu có `cluster` thì sai số chuẩn cluster theo cột đó (statsmodels, có hiệu chỉnh mẫu nhỏ)."""
    X = sm.add_constant(df[features].astype(float), has_constant="add")
    y = df[target].astype(float)
    model = sm.OLS(y, X)
    if cluster is None:
        return model.fit()
    return model.fit(cov_type="cluster", cov_kwds={"groups": df[cluster].to_numpy()})

def residual_diagnostics(res, model: str) -> dict:
    resid, fitted = np.asarray(res.resid), np.asarray(res.fittedvalues)
    jb, jb_p, skew, kurt = sm.stats.jarque_bera(resid)
    sw, sw_p = stats.shapiro(resid) if 3 <= len(resid) <= 5000 else (np.nan, np.nan)
    bp_lm, bp_p, _, _ = het_breuschpagan(resid, res.model.exog)
    return {
        "model": model, "n": int(res.nobs), "k": int(res.df_model),
        "r2": float(res.rsquared), "r2_adj": float(res.rsquared_adj),
        "f_pvalue": float(res.f_pvalue) if np.isfinite(res.f_pvalue) else None,
        "aic": float(res.aic), "bic": float(res.bic), "mse": float(np.mean(resid ** 2)),
        "rmse": float(np.sqrt(np.mean(resid ** 2))),
        "jarque_bera": float(jb), "jarque_bera_p": float(jb_p), "skew": float(skew), "kurtosis": float(kurt),
        "shapiro_w": float(sw), "shapiro_p": float(sw_p),
        "breusch_pagan_lm": float(bp_lm), "breusch_pagan_p": float(bp_p),
        "durbin_watson": float(durbin_watson(resid)),
        "condition_number": float(res.condition_number),
        "fitted_min": float(fitted.min()), "fitted_max": float(fitted.max()),
    }

def r2_score_pooled(y: np.ndarray, yhat: np.ndarray) -> float:
    """R² ngoài mẫu: 1 − SSE/SST, SST lấy quanh trung bình toàn bộ y (âm nếu dự báo tệ hơn hằng số)."""
    return float(1 - np.sum((y - yhat) ** 2) / np.sum((y - y.mean()) ** 2))

def _standardize(Xtr: np.ndarray, Xte: np.ndarray | None = None):
    mu, sd = Xtr.mean(0), Xtr.std(0)
    sd = np.where(sd > 0, sd, 1.0)
    return (Xtr - mu) / sd, (None if Xte is None else (Xte - mu) / sd), mu, sd

def _inner_splits(groups: np.ndarray, n_splits: int):
    k = min(n_splits, len(np.unique(groups)))
    return list(GroupKFold(n_splits=k).split(np.zeros(len(groups)), groups=groups))

def fit_elasticnet_cv(X: np.ndarray, y: np.ndarray, groups: np.ndarray, n_splits: int = cfg.CV_SPLITS,
                      seed: int = cfg.SEED) -> dict:
    """Chuẩn hóa X rồi chạy ElasticNetCV với các fold GroupKFold. Trả về mô hình + tham số chuẩn hóa."""
    Z, _, mu, sd = _standardize(X)
    en = ElasticNetCV(l1_ratio=list(cfg.EN_L1_RATIOS), **_n_alphas_kw(ElasticNetCV.__init__),
                      cv=_inner_splits(groups, n_splits), max_iter=50_000, random_state=seed)
    en.fit(Z, y)
    return {"model": en, "mean": mu, "scale": sd, "alpha": float(en.alpha_), "l1_ratio": float(en.l1_ratio_)}

def cross_validate(X: np.ndarray, y: np.ndarray, groups: np.ndarray, kind: str,
                   n_splits: int = cfg.CV_SPLITS, splitter: str = "group", seed: int = cfg.SEED) -> dict:
    """Dự báo ngoài mẫu (out-of-fold). kind: 'ols' | 'enet'. splitter: 'group' (đúng) | 'kfold' (rò rỉ, để so sánh)."""
    if splitter == "group":
        folds = group_folds(groups, n_splits, seed)
    else:
        folds = KFold(n_splits=n_splits, shuffle=True, random_state=seed).split(X, y)
    oof = np.full(len(y), np.nan)
    fold_r2, params = [], []
    for tr, te in folds:
        if kind == "ols":
            m = LinearRegression().fit(X[tr], y[tr])
            oof[te] = m.predict(X[te])
        else:
            if splitter == "group":
                fit = fit_elasticnet_cv(X[tr], y[tr], groups[tr], n_splits, seed)
            else:  # vòng trong cũng dùng KFold thường để mô phỏng đúng cách làm bị rò rỉ
                fit = fit_elasticnet_cv(X[tr], y[tr], np.arange(len(tr)), n_splits, seed)
            oof[te] = fit["model"].predict((X[te] - fit["mean"]) / fit["scale"])
            params.append({"alpha": fit["alpha"], "l1_ratio": fit["l1_ratio"]})
        fold_r2.append(r2_score_pooled(y[te], oof[te]) if len(te) > 1 else np.nan)
    return {
        "oof": oof, "r2_cv": r2_score_pooled(y, oof), "mse_cv": float(np.mean((y - oof) ** 2)),
        "rmse_cv": float(np.sqrt(np.mean((y - oof) ** 2))),
        "r2_cv_fold_mean": float(np.nanmean(fold_r2)), "fold_params": params,
    }

def elasticnet_path(X: np.ndarray, y: np.ndarray, l1_ratio: float):
    """Đường hệ số ElasticNet (trên X chuẩn hóa) theo alpha."""
    Z, _, _, _ = _standardize(X)
    alphas, coefs, _ = enet_path(Z, y - y.mean(), l1_ratio=l1_ratio, **_n_alphas_kw(enet_path), max_iter=50_000)
    return alphas, coefs  # coefs: (n_features, n_alphas)

@dataclass
class YieldResults:
    features: list[str]
    dropped: list[dict]
    ols63: object
    ols240: object
    coef: pd.DataFrame
    diagnostics: pd.DataFrame
    cv: pd.DataFrame
    enet: dict
    enet_coef: pd.DataFrame
    path: tuple[np.ndarray, np.ndarray]
    oof: dict

def run_yield(prepared: dict, features: list[str], dropped: list[dict], seed: int = cfg.SEED) -> YieldResults:
    d63, d240 = prepared["dedup"], prepared["all"]
    d63 = d63.dropna(subset=[*features, cfg.YIELD_TARGET])
    d240 = d240.dropna(subset=[*features, cfg.YIELD_TARGET])

    ols63 = fit_ols(d63, features)
    ols240 = fit_ols(d240, features, cluster=cfg.GROUP_COL)
    coef = pd.concat([coef_table(ols63, f"OLS_{len(d63)}_doc_lap"),
                      coef_table(ols240, f"OLS_{len(d240)}_cluster_group_id")], ignore_index=True)
    diag = pd.DataFrame([residual_diagnostics(ols63, f"OLS_{len(d63)}_doc_lap"),
                         residual_diagnostics(ols240, f"OLS_{len(d240)}_cluster_group_id")])

    X63, y63 = _design(d63, features, cfg.YIELD_TARGET)
    X240, y240 = _design(d240, features, cfg.YIELD_TARGET)
    g63, g240 = d63[cfg.GROUP_COL].to_numpy(), d240[cfg.GROUP_COL].to_numpy()

    en = fit_elasticnet_cv(X240, y240, g240, seed=seed)
    en_r2 = r2_score_pooled(y240, en["model"].predict((X240 - en["mean"]) / en["scale"]))
    coef_std = en["model"].coef_
    en_coef = pd.DataFrame({
        "term": features, "coef_standardized": coef_std, "coef_raw_unit": coef_std / en["scale"],
        "feature_mean": en["mean"], "feature_sd": en["scale"], "selected": np.abs(coef_std) > 1e-12,
    })

    rows, oofs = [], {}
    specs = [
        ("OLS", "ols", X63, y63, g63, "group", f"{len(d63)} lô độc lập, GroupKFold"),
        ("OLS", "ols", X240, y240, g240, "group", f"{len(d240)} lô, GroupKFold theo group_id"),
        ("ElasticNet", "enet", X63, y63, g63, "group", f"{len(d63)} lô độc lập, GroupKFold lồng nhau"),
        ("ElasticNet", "enet", X240, y240, g240, "group", f"{len(d240)} lô, GroupKFold lồng nhau theo group_id"),
        ("ElasticNet", "enet", X240, y240, g240, "kfold", f"{len(d240)} lô, KFold thường (RÒ RỈ, chỉ để so sánh)"),
    ]
    en_mse = float(np.mean((y240 - en["model"].predict((X240 - en["mean"]) / en["scale"])) ** 2))
    in_sample = {("OLS", len(d63)): (ols63.rsquared, float(np.mean(ols63.resid ** 2))),
                 ("OLS", len(d240)): (ols240.rsquared, float(np.mean(ols240.resid ** 2))),
                 ("ElasticNet", len(d240)): (en_r2, en_mse)}
    for name, kind, X, y, g, sp, desc in specs:
        # lặp kiểm định chéo với CV_REPEATS cách chia khác nhau; lần đầu (seed gốc) dùng để vẽ hình
        reps = [cross_validate(X, y, g, kind, splitter=sp, seed=seed + r) for r in range(cfg.CV_REPEATS)]
        r2s = np.array([r["r2_cv"] for r in reps])
        mses = np.array([r["mse_cv"] for r in reps])
        key = f"{name}_{len(y)}_{sp}"
        oofs[key] = {"y": y, "oof": reps[0]["oof"]}
        r2_in, mse_in = in_sample.get((name, len(y)), (None, None))
        rows.append({"model": name, "n": len(y), "cv": desc, "r2_in_sample": r2_in, "mse_in_sample": mse_in,
                     "r2_cv": float(r2s.mean()), "r2_cv_sd": float(r2s.std(ddof=1)),
                     "mse_cv": float(mses.mean()), "mse_cv_sd": float(mses.std(ddof=1)),
                     "rmse_cv": float(np.sqrt(mses).mean()), "n_repeats": cfg.CV_REPEATS,
                     "var_y": float(np.var(y)), "key": key})
    path = elasticnet_path(X240, y240, en["l1_ratio"])
    return YieldResults(features, dropped, ols63, ols240, coef, diag, pd.DataFrame(rows),
                        {**en, "r2_in_sample": en_r2, "mse_in_sample": en_mse, "n": len(y240)}, en_coef, path, oofs)

def export_yield_pipeline(res: YieldResults, df: pd.DataFrame):
    """Pipeline sklearn (median impute -> chuẩn hóa -> ElasticNet với tham số đã chọn), xuất ra yield_model.joblib
    (Bước 4 kế hoạch). Fit trên DataFrame nên pipeline nhớ tên cột (feature_names_in_)."""
    from sklearn.impute import SimpleImputer
    from sklearn.pipeline import Pipeline
    from sklearn.preprocessing import StandardScaler

    pipe = Pipeline([("impute", SimpleImputer(strategy="median")), ("scale", StandardScaler()),
                     ("enet", ElasticNet(alpha=res.enet["alpha"], l1_ratio=res.enet["l1_ratio"], max_iter=50_000))])
    d = df.dropna(subset=[cfg.YIELD_TARGET])
    return pipe.fit(d[res.features].astype(float), d[cfg.YIELD_TARGET].astype(float))