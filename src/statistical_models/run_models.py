"""Pipeline: chạy 3 bài toán trên dữ liệu Gold, ghi bảng/hình vào --out và mô hình vào --models-dir.

    $env:PYTHONPATH = "src"
    python -m statistical_models.run_models `
        --sensor-path data/03_gold_marts/fact_sensor_telemetry.parquet `
        --features-path data/03_gold_marts/batch_features.parquet `
        --out reports/models --models-dir models
"""

from __future__ import annotations

import argparse
import json
import sys
import time
from pathlib import Path

import joblib
import numpy as np
import pandas as pd

from . import config as cfg
from . import data
from . import figures as F
from .em_missing import fit_final, run_em_experiment
from .linear_models import export_yield_pipeline, run_yield
from .logistic import export_defect_pipeline, run_defect

def _csv(df: pd.DataFrame, path: Path, index: bool = False) -> None:
    df.to_csv(path, index=index, encoding="utf-8-sig")  # mở bằng Excel không lỗi tiếng Việt

def _json(obj, path: Path) -> None:
    path.write_text(json.dumps(obj, indent=2, ensure_ascii=False, default=_default), encoding="utf-8")

def _default(o):
    if isinstance(o, np.ndarray):
        return o.tolist()
    if isinstance(o, (np.integer,)):
        return int(o)
    if isinstance(o, (np.floating,)):
        return None if not np.isfinite(o) else float(o)
    if isinstance(o, (np.bool_,)):
        return bool(o)
    raise TypeError(type(o))

def _records(df: pd.DataFrame) -> list[dict]:
    return json.loads(df.to_json(orient="records", force_ascii=False))

def bai1(prep: dict, out: Path, models: Path, figs: bool) -> dict:
    feats, dropped = data.usable_features(prep["all"], cfg.YIELD_FEATURES)
    r = run_yield(prep, feats, dropped)
    _csv(r.coef, out / "yield_coefficients.csv")
    _csv(r.diagnostics, out / "yield_residual_diagnostics.csv")
    _csv(r.cv.drop(columns="key"), out / "yield_cv_scores.csv")
    _csv(r.enet_coef, out / "yield_elasticnet_coef.csv")
    alphas, coefs = r.path
    _csv(pd.DataFrame(coefs.T, columns=feats).assign(alpha=alphas), out / "yield_elasticnet_path.csv")

    cv = r.cv.set_index("key")
    k63, k240 = f"OLS_{int(r.ols63.nobs)}_group", f"ElasticNet_{r.enet['n']}_group"
    pipe = export_yield_pipeline(r, prep["all"])
    joblib.dump(pipe, models / "yield_model.joblib")
    _json({
        "model": "yield_recovery_pct ~ thông số công nghệ + chất lượng sữa + thời tiết + thị trường",
        "target": cfg.YIELD_TARGET, "features": feats, "dropped_features": dropped,
        "data": {"source": "G batch_features.parquet (+ homo_press1_avg gộp từ G fact_sensor_telemetry)",
                 "scope": f"lô năm {cfg.YEAR}, n_readings > 0", **prep["summary"]},
        "ols_independent": {"n": int(r.ols63.nobs), "r2": r.ols63.rsquared, "r2_adj": r.ols63.rsquared_adj,
                            "mse": float(np.mean(r.ols63.resid ** 2)),
                            "r2_cv_groupkfold": cv.loc[k63, "r2_cv"], "r2_cv_sd": cv.loc[k63, "r2_cv_sd"],
                            "mse_cv_groupkfold": cv.loc[k63, "mse_cv"],
                            "coefficients": _records(r.coef[r.coef.model.str.startswith(f"OLS_{int(r.ols63.nobs)}_")]
                                                     .drop(columns="model"))},
        "ols_cluster": {"n": int(r.ols240.nobs), "r2": r.ols240.rsquared, "cov_type": "cluster theo group_id",
                        "coefficients": _records(r.coef[r.coef.model.str.contains("cluster")].drop(columns="model"))},
        "elasticnet": {"n": r.enet["n"], "alpha": r.enet["alpha"], "l1_ratio": r.enet["l1_ratio"],
                       "intercept": float(r.enet["model"].intercept_), "r2_in_sample": r.enet["r2_in_sample"],
                       "mse_in_sample": r.enet["mse_in_sample"],
                       "r2_cv_nested_groupkfold": cv.loc[k240, "r2_cv"], "r2_cv_sd": cv.loc[k240, "r2_cv_sd"],
                       "mse_cv_nested_groupkfold": cv.loc[k240, "mse_cv"], "mse_cv_sd": cv.loc[k240, "mse_cv_sd"],
                       "cv_repeats": cfg.CV_REPEATS,
                       "coefficients": _records(r.enet_coef),
                       "n_selected": int(r.enet_coef["selected"].sum())},
        "residual_diagnostics": _records(r.diagnostics),
        "cv_scores": _records(r.cv.drop(columns="key")),
        "joblib": "models/yield_model.joblib (Pipeline: median impute -> StandardScaler -> ElasticNet)",
        "interpretation": (
            "R² thấp và R² kiểm định chéo ≈ 0 hoặc âm: các biến công nghệ/thời tiết không giải thích được tỷ lệ thu hồi. "
            "Đúng với bộ sinh dữ liệu (generate_mes_data.py): yield chỉ phụ thuộc cờ lỗi (95 ± 2 nếu đạt, 88 ± 3 nếu lỗi)."),
    }, models / "yield_model_real.json")
    if figs:
        F.fig_yield_residuals(r.ols63, out / "figures")
        F.fig_enet_path(alphas, coefs, feats, r.enet["alpha"], r.enet["l1_ratio"], out / "figures")
        o = r.oof[k240]
        F.fig_yield_cv(o["y"], o["oof"], f"ElasticNet, GroupKFold lồng nhau (R²cv = {cv.loc[k240, 'r2_cv']:.3f} ± {cv.loc[k240, 'r2_cv_sd']:.3f})",
                       out / "figures")
    return {"features": feats, "dropped": dropped,
            "r2_ols_doc_lap": r.ols63.rsquared, "r2_ols_cluster": r.ols240.rsquared,
            "cv": {k: {"r2_cv": round(cv.loc[k, "r2_cv"], 4), "r2_cv_sd": round(cv.loc[k, "r2_cv_sd"], 4),
                       "mse_cv": round(cv.loc[k, "mse_cv"], 4)} for k in cv.index},
            "mse_elasticnet_trong_mau": r.enet["mse_in_sample"],
            "elasticnet": {"alpha": r.enet["alpha"], "l1_ratio": r.enet["l1_ratio"],
                           "so_bien_giu_lai": int(r.enet_coef["selected"].sum())}}

def bai2(prep: dict, out: Path, models: Path, figs: bool) -> dict:
    feats, dropped = data.usable_features(prep["all"], cfg.DEFECT_FEATURES)
    r = run_defect(prep, feats)
    _csv(r.or_tables, out / "defect_odds_ratios.csv")
    _csv(r.comparison, out / "defect_irls_vs_statsmodels.csv")
    _csv(r.metrics, out / "defect_metrics.csv")
    _csv(r.folds, out / "defect_cv_folds.csv")
    for k, t in r.hl_tables.items():
        _csv(t, out / f"defect_hosmer_lemeshow_{k}.csv")
    roc_rows = [pd.DataFrame({"sample": s, "scope": sc, "fpr": v["fpr"], "tpr": v["tpr"],
                              "threshold": np.minimum(v["thresholds"], 1.0)})
                for s, d in r.roc.items() for sc, v in d.items()]
    _csv(pd.concat(roc_rows, ignore_index=True), out / "defect_roc_points.csv")
    main = next(k for k in r.fits if k.endswith("cluster"))
    f = r.fits[main]
    d = prep["all"].dropna(subset=[*feats, cfg.DEFECT_TARGET])
    _csv(d[["batch_key", "batch_id", cfg.GROUP_COL, *feats, cfg.DEFECT_TARGET]]
         .assign(p_in_sample=f["p_in"], p_oof=f["p_oof"]), out / "defect_predictions.csv")

    pipe = export_defect_pipeline(prep["all"], feats)
    joblib.dump(pipe, models / "defect_model.joblib")
    met = r.metrics.set_index(["sample", "scope"])
    _json({
        "model": "Logit P(batch_defect_flag = 1 | x), MLE bằng IRLS tự viết, đối chiếu statsmodels",
        "target": cfg.DEFECT_TARGET, "features": feats, "dropped_features": dropped,
        "feature_source": "reports/spc/correlation_target.csv (TV5): Spearman với cờ lỗi, q < 0,05",
        "standardization": {"mean": dict(zip(feats, f["mean"])), "sd": dict(zip(feats, f["scale"]))},
        "beta_standardized": dict(zip(["const", *feats], f["irls"].beta)),
        "main_sample": main,
        "odds_ratios": _records(r.or_tables),
        "irls_vs_statsmodels_max_abs_diff": {
            "beta": float((r.comparison.beta_irls - r.comparison.beta_statsmodels).abs().max()),
            "se": float((r.comparison.se_irls - r.comparison.se_statsmodels).abs().max()),
            "se_cluster": float((r.comparison.se_cluster_irls - r.comparison.se_cluster_statsmodels).abs().max())},
        "metrics": _records(r.metrics),
        "threshold_youden_oof": met.loc[(main, "oof_stratified_group_kfold"), "threshold_youden"],
        "auc_oof": met.loc[(main, "oof_stratified_group_kfold"), "auc"],
        "auc_oof_repeated": {"mean": met.loc[(main, "oof_stratified_group_kfold"), "auc_cv_mean"],
                             "sd": met.loc[(main, "oof_stratified_group_kfold"), "auc_cv_sd"],
                             "n_repeats": cfg.CV_REPEATS},
        "joblib": "models/defect_model.joblib (Pipeline: StandardScaler -> LogisticRegression(penalty=None) = MLE)",
        "caveats": [
            f"Chỉ có {prep['summary']['n_defect_groups']} lô lỗi độc lập ({prep['summary']['n_defect_batches']} dòng "
            "lỗi là các bản sao) nên KTC của odds ratio rất rộng; SE cluster với rất ít cụm có sự kiện thường bị "
            "ước lượng thấp, nên đọc thêm SE MLE của mẫu lô độc lập.",
            "Hosmer–Lemeshow giả định các quan sát độc lập: trên mẫu có bản sao, kết quả không đáng tin; dùng p của mẫu "
            "lô độc lập.",
            "Dùng StratifiedGroupKFold (GroupKFold có phân tầng) để fold kiểm tra nào cũng có lô lỗi; "
            "bản sao của một lô không bao giờ nằm ở cả train và test."],
    }, models / "defect_model_real.json")
    if figs:
        F.fig_roc(r.roc, out / "figures")
        ind = next(k for k in r.fits if k.endswith("doc_lap"))
        hl_t = r.hl_tables[f"{ind}_in_sample"]
        F.fig_hosmer(hl_t, {"p_value": met.loc[(ind, "in_sample"), "hl_p_value"]}, ind, out / "figures")
    return {"features": feats, "metrics": _records(r.metrics[["sample", "scope", "auc", "auc_cv_mean", "auc_cv_sd",
                                                               "threshold_youden",
                                                               "sensitivity", "specificity", "hl_p_value"]]),
            "odds_ratio_per_sd": _records(r.or_tables[["model", "term", "or_per_sd", "or_per_sd_ci_low",
                                                       "or_per_sd_ci_high", "p_value"]])}

def bai3(minutes: pd.DataFrame, out: Path, models: Path, figs: bool) -> dict:
    # 1) validate: µ, Σ học trên các lô train, chấm điểm trên các lô valid (2 kịch bản mất dữ liệu)
    r = run_em_experiment(minutes)
    cols, em = r["cols"], r["em"]
    _csv(r["rmse_z"], out / "em_rmse_zscore.csv")
    _csv(r["rmse_raw"], out / "em_rmse_original_units.csv")
    _csv(pd.DataFrame({"iteration": np.arange(len(em.history)), "observed_loglik": em.history}),
         out / "em_loglik_history.csv")
    for name, filled in r["filled_valid"].items():
        mk = pd.DataFrame(r["masks_valid"][name], columns=[f"{c}_was_missing" for c in cols])
        _csv(pd.concat([r["valid"][["batch_key", "minute"]], pd.DataFrame(filled, columns=cols), mk], axis=1),
             out / f"em_filled_{name}_zscore.csv")
    # 2) bàn giao: học lại µ, Σ trên toàn bộ dữ liệu phút (giống Bài 1, 2 học lại trên toàn bộ dữ liệu)
    fin = fit_final(minutes)
    rz = r["rmse_z"].set_index(["tap", "bien"])
    scen = {"train": "train (MCAR 10 %)", "valid_mcar": "valid, MCAR 10 %",
            "valid_mat_tin_hieu": f"valid, mỗi lô {cfg.EM_OUTAGES_PER_BATCH} cảm biến mất liền "
                                  f"{cfg.EM_OUTAGE_MIN}–{cfg.EM_OUTAGE_MAX} phút"}
    total = {k: {m: (None if pd.isna(v) else float(v)) for m, v in rz.loc[(k, "TOAN_BO")].items() if m != "so_o_bi_xoa"}
             | {"so_o_bi_xoa": int(rz.loc[(k, "TOAN_BO"), "so_o_bi_xoa"]), "mo_ta": d}
             for k, d in scen.items()}
    _json({
        "model": "EM cho N(µ, Σ) với dữ liệu khuyết (NumPy thuần)",
        "variables": cols, "data": "trung bình theo phút từng lô, G fact_sensor_telemetry, năm 2025",
        "final_fit": {"rows": fin["n_rows"], "missing_cells_in_gold": fin["n_missing_cells"],
                      "n_iter": fin["em"].n_iter,
                      "note": "µ, Σ bàn giao học trên toàn bộ dữ liệu phút sau khi đã validate."},
        "mu_hat_original": dict(zip(cols, fin["mu"])), "sigma_hat_original": fin["sigma"],
        "zscore": {"mean": dict(zip(cols, fin["z_mean"])), "sd": dict(zip(cols, fin["z_sd"]))},
        "mu_hat_z": dict(zip(cols, fin["em"].mu)), "sigma_hat_z": fin["em"].sigma,
        "validation": {
            "split": {"by": "batch_key", "valid_fraction": cfg.EM_VALID_FRACTION, **r["split"]},
            "missing_cells": r["missing_cells"], "rmse_zscore_total": total,
            "rmse_zscore": _records(r["rmse_z"]), "n_iter_train": em.n_iter,
            "loglik_monotone": r["loglik_monotone"], "loglik_first_last": [em.history[0], em.history[-1]],
            "param_error_vs_full_train": r["param_error"]},
        "usage": "statistical_models.em_missing.conditional_impute(X, mu, sigma): "
                 "x_m = µ_m + Σ_mo Σ_oo⁻¹ (x_o − µ_o), dùng µ, Σ đơn vị gốc ở trên.",
    }, models / "em_sensor_mvn.json")
    if figs:
        F.fig_em_loglik(em.history, out / "figures")
        F.fig_em_rmse(r["rmse_z"], out / "figures")
    return {"chia": r["split"], **{k: {m: round(v, 4) if isinstance(v, float) else v for m, v in t.items()}
                                   for k, t in total.items()},
            "n_iter": em.n_iter, "loglik_khong_giam": r["loglik_monotone"]}

def run(sensor: pd.DataFrame, batches: pd.DataFrame, out: Path, models: Path, figs: bool = True,
        tasks: tuple[int, ...] = (1, 2, 3)) -> dict:
    out.mkdir(parents=True, exist_ok=True)
    (out / "figures").mkdir(exist_ok=True)
    models.mkdir(parents=True, exist_ok=True)
    t0 = time.perf_counter()
    prep = data.prepare_batches(batches)
    _csv(prep["all"][["batch_key", "batch_id", cfg.GROUP_COL, *cfg.DEDUP_KEYS]], out / "batch_groups.csv")
    # chạy một phần (--tasks) thì giữ kết quả các bài khác trong summary.json
    old = out / "summary.json"
    keep = ("bai1_yield", "bai2_defect", "bai3_em", "so_phut")
    res = {k: v for k, v in (json.loads(old.read_text(encoding="utf-8")) if old.exists() else {}).items() if k in keep}
    res["chuan_bi"] = prep["summary"]
    if 1 in tasks:
        res["bai1_yield"] = bai1(prep, out, models, figs)
    if 2 in tasks:
        res["bai2_defect"] = bai2(prep, out, models, figs)
    if 3 in tasks:
        minutes = data.minute_means(sensor)
        res["so_phut"] = int(len(minutes))
        res["bai3_em"] = bai3(minutes, out, models, figs)
    res["thoi_gian_s"] = round(time.perf_counter() - t0, 1)
    _json(res, out / "summary.json")
    return res

def main() -> None:
    if hasattr(sys.stdout, "reconfigure"):
        sys.stdout.reconfigure(encoding="utf-8")  # console Windows mặc định cp1252
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--sensor-path", default="data/03_gold_marts/fact_sensor_telemetry.parquet")
    ap.add_argument("--features-path", default="data/03_gold_marts/batch_features.parquet")
    ap.add_argument("--out", default="reports/models")
    ap.add_argument("--models-dir", default="models")
    ap.add_argument("--tasks", default="1,2,3", help="chạy một phần: 1 yield, 2 lô lỗi, 3 EM; ví dụ --tasks 1,2")
    ap.add_argument("--no-figs", action="store_true")
    a = ap.parse_args()
    import warnings
    warnings.filterwarnings("ignore", category=FutureWarning)
    sensor, batches = data.load_gold(a.sensor_path, a.features_path)
    tasks = tuple(int(t) for t in a.tasks.split(","))
    res = run(sensor, batches, Path(a.out), Path(a.models_dir), figs=not a.no_figs, tasks=tasks)
    print(json.dumps(res, indent=2, ensure_ascii=False, default=_default))
    print(f"\nĐã ghi kết quả vào {Path(a.out).resolve()} và mô hình vào {Path(a.models_dir).resolve()}")

if __name__ == "__main__":
    main()