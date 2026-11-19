"""Bài 2: dự báo lô lỗi batch_defect_flag bằng hồi quy Logistic ước lượng MLE.

    P(y=1|x) = 1 / (1 + exp(−xᵀβ))
    β(t+1)   = β(t) + (XᵀWX)⁻¹ Xᵀ(y − p),   W = diag(p(1 − p))     (Newton–Raphson = IRLS)

- irls(): tự viết bằng NumPy, có giảm bước (step-halving) để log-likelihood không bao giờ giảm.
- Đối chiếu với statsmodels Logit (hệ số, sai số chuẩn thường và cluster theo group_id).
- Ngoài mẫu: StratifiedGroupKFold (GroupKFold có phân tầng: các bản sao của một lô luôn nằm cùng
  một fold, và fold nào cũng có lô lỗi), ROC/AUC, ngưỡng Youden J, Hosmer–Lemeshow.
"""

from __future__ import annotations

from dataclasses import dataclass, field

import numpy as np
import pandas as pd
from scipy import stats
from scipy.special import expit
from sklearn.metrics import roc_auc_score, roc_curve
from sklearn.model_selection import StratifiedGroupKFold

from . import config as cfg

def log_likelihood(X: np.ndarray, y: np.ndarray, beta: np.ndarray) -> float:
    eta = X @ beta
    return float(np.sum(y * eta - np.logaddexp(0.0, eta)))  # ổn định số khi |eta| lớn

@dataclass
class IRLSResult:
    beta: np.ndarray
    cov: np.ndarray            # (XᵀWX)⁻¹ tại nghiệm: ma trận hiệp phương sai MLE
    loglik: float
    n_iter: int
    converged: bool
    history: list[float] = field(default_factory=list)
    separation_warning: bool = False

    @property
    def se(self) -> np.ndarray:
        return np.sqrt(np.diag(self.cov))

def irls(X: np.ndarray, y: np.ndarray, max_iter: int = 100, tol: float = 1e-10) -> IRLSResult:
    """Newton–Raphson / IRLS cho Logistic. X đã có cột hệ số chặn."""
    X, y = np.asarray(X, float), np.asarray(y, float)
    beta = np.zeros(X.shape[1])
    ll = log_likelihood(X, y, beta)
    history, converged, it = [ll], False, 0
    for it in range(1, max_iter + 1):
        p = expit(X @ beta)
        w = p * (1 - p)
        H = X.T @ (X * w[:, None])            # XᵀWX (Fisher information)
        if np.linalg.cond(H) > 1e12:          # tách biệt hoàn toàn: W -> 0, XᵀWX suy biến, MLE không tồn tại
            break
        step = np.linalg.solve(H, X.T @ (y - p))
        t = 1.0
        while True:  # giảm bước nếu bước Newton đầy đủ làm log-likelihood giảm
            new = beta + t * step
            new_ll = log_likelihood(X, y, new)
            if new_ll >= ll - 1e-12 or t < 1e-10:
                break
            t /= 2
        beta, delta = new, np.max(np.abs(t * step))
        history.append(new_ll)
        done = delta < tol or abs(new_ll - ll) < tol * (abs(ll) + tol)
        ll = new_ll
        if done:
            converged = True
            break
    p = expit(X @ beta)
    w = p * (1 - p)
    cov = np.linalg.pinv(X.T @ (X * w[:, None]))
    # dấu hiệu tách biệt (gần) hoàn toàn: hệ số rất lớn trên thang chuẩn hóa, xác suất dồn về 0/1
    sep = bool(np.max(np.abs(beta[1:])) > 10 or (not converged)
               or (np.any((y == 1) & (p > 1 - 1e-6)) and np.any((y == 0) & (p < 1e-6))))
    return IRLSResult(beta, cov, ll, it, converged, history, sep)

def cluster_cov(X: np.ndarray, y: np.ndarray, beta: np.ndarray, groups: np.ndarray) -> np.ndarray:
    """Sai số chuẩn cluster (sandwich): B⁻¹ (Σ_g s_g s_gᵀ) B⁻¹ · G/(G−1) · (N−1)/(N−K), giống statsmodels."""
    p = expit(X @ beta)
    bread = np.linalg.pinv(X.T @ (X * (p * (1 - p))[:, None]))
    scores = X * (y - p)[:, None]
    gs = pd.DataFrame(scores).groupby(np.asarray(groups)).sum().to_numpy()
    meat = gs.T @ gs
    n, k, G = X.shape[0], X.shape[1], gs.shape[0]
    corr = G / (G - 1) * (n - 1) / (n - k)
    return corr * bread @ meat @ bread

def or_table(beta: np.ndarray, cov: np.ndarray, names: list[str], scale: np.ndarray, model: str) -> pd.DataFrame:
    """Odds ratio cho 1 độ lệch chuẩn (thang đã chuẩn hóa) và cho 1 đơn vị gốc."""
    se = np.sqrt(np.diag(cov))
    z = beta / se
    q = stats.norm.ppf(0.975)
    sc = np.r_[1.0, scale]
    return pd.DataFrame({
        "model": model, "term": ["const", *names], "beta_std": beta, "se_std": se, "z": z,
        "p_value": 2 * stats.norm.sf(np.abs(z)),
        "or_per_sd": np.exp(beta), "or_per_sd_ci_low": np.exp(beta - q * se), "or_per_sd_ci_high": np.exp(beta + q * se),
        "beta_per_unit": beta / sc, "or_per_unit": np.exp(beta / sc),
        "or_per_unit_ci_low": np.exp((beta - q * se) / sc), "or_per_unit_ci_high": np.exp((beta + q * se) / sc),
    })

def hosmer_lemeshow(y: np.ndarray, p: np.ndarray, g: int = cfg.HL_GROUPS) -> dict:
    """Kiểm định Hosmer–Lemeshow: chia theo thập phân vị xác suất dự báo, so số lỗi quan sát và kỳ vọng."""
    order = np.argsort(p, kind="mergesort")
    rows = []
    for idx in np.array_split(order, g):
        if len(idx) == 0:
            continue
        n_g, o1, e1 = len(idx), float(y[idx].sum()), float(p[idx].sum())
        rows.append({"n": n_g, "p_mean": float(p[idx].mean()), "observed_1": o1, "expected_1": e1,
                     "observed_0": n_g - o1, "expected_0": n_g - e1})
    t = pd.DataFrame(rows)
    with np.errstate(divide="ignore", invalid="ignore"):
        terms = (t.observed_1 - t.expected_1) ** 2 / t.expected_1 + (t.observed_0 - t.expected_0) ** 2 / t.expected_0
    stat = float(np.nansum(terms))
    df = len(t) - 2
    return {"statistic": stat, "df": df, "p_value": float(stats.chi2.sf(stat, df)), "table": t}

def roc_youden(y: np.ndarray, p: np.ndarray) -> dict:
    fpr, tpr, thr = roc_curve(y, p)
    j = tpr - fpr
    i = int(np.argmax(j))
    pred = p >= thr[i]
    tp, fn = int((pred & (y == 1)).sum()), int((~pred & (y == 1)).sum())
    tn, fp = int((~pred & (y == 0)).sum()), int((pred & (y == 0)).sum())
    return {"auc": float(roc_auc_score(y, p)), "fpr": fpr, "tpr": tpr, "thresholds": thr,
            "youden_j": float(j[i]), "threshold": float(min(thr[i], 1.0)),
            "sensitivity": tp / max(tp + fn, 1), "specificity": tn / max(tn + fp, 1),
            "confusion": {"tp": tp, "fn": fn, "tn": tn, "fp": fp}}

def _std_design(Xtr: np.ndarray, Xte: np.ndarray | None = None):
    mu, sd = Xtr.mean(0), Xtr.std(0)
    sd = np.where(sd > 0, sd, 1.0)
    add = lambda Z: np.column_stack([np.ones(len(Z)), Z])  # noqa: E731
    return add((Xtr - mu) / sd), (None if Xte is None else add((Xte - mu) / sd)), mu, sd

def oof_predictions(X: np.ndarray, y: np.ndarray, groups: np.ndarray, n_splits: int = cfg.DEFECT_CV_SPLITS,
                    seed: int = cfg.SEED) -> tuple[np.ndarray, list[dict]]:
    """Xác suất dự báo ngoài mẫu; chuẩn hóa và IRLS chỉ học trên fold huấn luyện."""
    n_pos_groups = len(np.unique(groups[y == 1]))
    k = max(2, min(n_splits, n_pos_groups))
    oof = np.full(len(y), np.nan)
    info = []
    for f, (tr, te) in enumerate(StratifiedGroupKFold(n_splits=k, shuffle=True, random_state=seed)
                                 .split(X, y, groups)):
        Xtr, Xte, _, _ = _std_design(X[tr], X[te])
        r = irls(Xtr, y[tr])
        oof[te] = expit(Xte @ r.beta)
        info.append({"fold": f, "n_train": len(tr), "n_test": len(te), "defects_train": int(y[tr].sum()),
                     "defects_test": int(y[te].sum()), "converged": r.converged,
                     "separation_warning": r.separation_warning})
    return oof, info

def statsmodels_logit(Xd: np.ndarray, y: np.ndarray, groups: np.ndarray | None = None):
    import statsmodels.api as sm

    m = sm.Logit(y, Xd)
    if groups is None:
        return m.fit(disp=0, method="newton", maxiter=200)
    return m.fit(disp=0, method="newton", maxiter=200, cov_type="cluster", cov_kwds={"groups": groups})

def compare_with_statsmodels(Xd: np.ndarray, y: np.ndarray, groups: np.ndarray, res: IRLSResult,
                             cov_cl: np.ndarray, names: list[str], model: str) -> pd.DataFrame:
    sm_plain = statsmodels_logit(Xd, y)
    sm_cl = statsmodels_logit(Xd, y, groups)
    return pd.DataFrame({
        "model": model, "term": ["const", *names],
        "beta_irls": res.beta, "beta_statsmodels": np.asarray(sm_plain.params),
        "se_irls": res.se, "se_statsmodels": np.asarray(sm_plain.bse),
        "se_cluster_irls": np.sqrt(np.diag(cov_cl)), "se_cluster_statsmodels": np.asarray(sm_cl.bse),
        "loglik_irls": res.loglik, "loglik_statsmodels": float(sm_plain.llf),
    })

@dataclass
class DefectResults:
    features: list[str]
    fits: dict                 # theo tên mẫu: {"irls", "cov_cluster", "mean", "scale", "p", "y", ...}
    or_tables: pd.DataFrame
    comparison: pd.DataFrame
    metrics: pd.DataFrame
    hl_tables: dict
    roc: dict
    folds: pd.DataFrame

def run_defect(prepared: dict, features: list[str], seed: int = cfg.SEED) -> DefectResults:
    samples = {"cluster": prepared["all"], "doc_lap": prepared["dedup"]}
    fits, ors, comps, mets, hls, rocs, folds = {}, [], [], [], {}, {}, []
    for kind, df in samples.items():
        d = df.dropna(subset=[*features, cfg.DEFECT_TARGET])
        name = f"{len(d)}_lo_{kind}"
        X, y, g = d[features].to_numpy(float), d[cfg.DEFECT_TARGET].to_numpy(float), d[cfg.GROUP_COL].to_numpy()
        Xd, _, mu, sd = _std_design(X)
        r = irls(Xd, y)
        cov_cl = cluster_cov(Xd, y, r.beta, g)
        use_cluster = name.endswith("cluster")
        cov = cov_cl if use_cluster else r.cov
        p_in = expit(Xd @ r.beta)
        p_oof, info = oof_predictions(X, y, g, seed=seed)
        auc_reps = [roc_auc_score(y, p_oof)] + [roc_auc_score(y, oof_predictions(X, y, g, seed=seed + r)[0])
                                                for r in range(1, cfg.CV_REPEATS)]
        ors.append(or_table(r.beta, cov, features, sd, f"{name} ({'SE cluster' if use_cluster else 'SE MLE'})"))
        comps.append(compare_with_statsmodels(Xd, y, g, r, cov_cl, features, name))
        roc_in, roc_oof = roc_youden(y, p_in), roc_youden(y, p_oof)
        hl_in, hl_oof = hosmer_lemeshow(y, p_in), hosmer_lemeshow(y, p_oof)
        hls[f"{name}_in_sample"], hls[f"{name}_oof"] = hl_in["table"], hl_oof["table"]
        rocs[name] = {"in_sample": roc_in, "oof": roc_oof}
        folds += [{"sample": name, **i} for i in info]
        for scope, rr, hl, pp in (("in_sample", roc_in, hl_in, p_in), ("oof_stratified_group_kfold", roc_oof, hl_oof, p_oof)):
            mets.append({
                "sample": name, "scope": scope, "n": len(y), "n_defect": int(y.sum()),
                "n_groups": int(len(np.unique(g))), "n_defect_groups": int(len(np.unique(g[y == 1]))),
                "auc": rr["auc"],
                "auc_cv_mean": float(np.mean(auc_reps)) if scope != "in_sample" else None,
                "auc_cv_sd": float(np.std(auc_reps, ddof=1)) if scope != "in_sample" else None,
                "n_repeats": cfg.CV_REPEATS if scope != "in_sample" else None,
                "youden_j": rr["youden_j"], "threshold_youden": rr["threshold"],
                "sensitivity": rr["sensitivity"], "specificity": rr["specificity"],
                "brier": float(np.mean((pp - y) ** 2)), "hl_statistic": hl["statistic"], "hl_df": hl["df"],
                "hl_p_value": hl["p_value"], "irls_iter": r.n_iter, "irls_converged": r.converged,
                "separation_warning": r.separation_warning, "loglik": r.loglik,
                "mcfadden_r2": 1 - r.loglik / log_likelihood(Xd[:, :1], y, np.array([np.log(y.mean() / (1 - y.mean()))])),
            })
        fits[name] = {"irls": r, "cov_cluster": cov_cl, "mean": mu, "scale": sd, "p_in": p_in, "p_oof": p_oof,
                      "y": y, "groups": g, "use_cluster": use_cluster}
    return DefectResults(features, fits, pd.concat(ors, ignore_index=True), pd.concat(comps, ignore_index=True),
                         pd.DataFrame(mets), hls, rocs, pd.DataFrame(folds))

def export_defect_pipeline(df: pd.DataFrame, features: list[str]):
    """Pipeline sklearn (chuẩn hóa -> LogisticRegression không phạt = MLE), xuất ra defect_model.joblib
    (Bước 4 kế hoạch); hệ số trùng với IRLS."""
    from sklearn.linear_model import LogisticRegression
    from sklearn.pipeline import Pipeline
    from sklearn.preprocessing import StandardScaler

    d = df.dropna(subset=[*features, cfg.DEFECT_TARGET])
    # C = vô cùng: không phạt, tương đương MLE (penalty=None bị deprecate từ sklearn 1.8)
    pipe = Pipeline([("scale", StandardScaler()),
                     ("logit", LogisticRegression(C=np.inf, solver="newton-cg", max_iter=10_000, tol=1e-10))])
    return pipe.fit(d[features].astype(float), d[cfg.DEFECT_TARGET].astype(int))