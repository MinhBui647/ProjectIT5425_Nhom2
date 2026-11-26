"""Pipeline: chạy 2 bài (1 giám sát / lô cần kiểm tra, 2 điền dữ liệu cảm biến bị mất) trên Gold,
ghi bảng/hình vào --out và mô hình vào --models-dir.

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

import numpy as np
import pandas as pd

from . import config as cfg
from . import data
from . import figures as F
from .em_missing import band_coverage, choose_fill_method, fit_final, outage_action, run_em_experiment
from .hotelling import run_monitoring

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

def giam_sat(minutes: pd.DataFrame, batches: pd.DataFrame, out: Path, models: Path, figs: bool) -> dict:
    """Bài 1: Hotelling T², train Phase I, validate Phase II."""
    r = run_monitoring(minutes, batches)
    cols = r["cols"]
    faults = r["faults"].drop(columns="nhan")
    _csv(r["minute_level"], out / "t2_bao_dong_theo_phut.csv")
    _csv(r["batch_level"], out / "t2_gan_co_theo_lo.csv")
    _csv(faults, out / "t2_loi_gia_lap.csv")
    _csv(r["inspection_list"], out / "lo_can_kiem_tra_phase2.csv")
    lst = r["inspection_list"]
    _json({
        "model": "Hotelling T² = (x − µ)ᵀ Σ⁻¹ (x − µ) trên trung bình 1 phút của 7 cảm biến; µ, Σ học bằng EM",
        "variables": cols, "alpha": r["alpha"], "limit_chi2_full": r["limit_full"],
        "rule_lo_can_kiem_tra": {
            "k": r["k_t2"],
            "mo_ta": f"≥ {r['k_t2']} phút T² vượt ngưỡng thì cần kiểm tra; k chọn trên Phase I",
            "khi_mat_cam_bien": "T² tính trên các cảm biến còn đo được"},
        "mu_original": dict(zip(cols, r["mu"])), "sigma_original": r["sigma"],
        "train_phase1": r["phase1"], "validate_phase2": r["phase2"],
        "validation": {"minute_level": _records(r["minute_level"]), "batch_level": _records(r["batch_level"]),
                       "outage": r["outage"], "fault_injection": _records(faults),
                       "simulation": r["simulation"]},
        "usage": "hotelling.t2_scores(X, mu, sigma); gắn cờ lô khi số phút signal ≥ k",
    }, models / "monitoring_t2.json")
    ml = r["minute_level"].set_index(["phuong_phap", "tap"])["ti_le_bao_dong"]
    if figs:
        F.fig_t2_distribution(r["t2_phase2"], len(cols), r["limit_full"],
                              float(ml.loc[("Hotelling T²", "Phase II, lô đạt")]), r["alpha"], out / "figures")
        F.fig_t2_detection(r["faults"], r["batch_level"], out / "figures")
    show = [c for c in ("batch_id", "n_signals", "cam_bien_gop_nhieu_nhat", cfg.DEFECT_TARGET) if c in lst]
    return {"train_phase1": r["phase1"], "validate_phase2": r["phase2"], "k": r["k_t2"],
            "k_3sigma": r["k_univariate"],
            "bao_dong_gia_theo_phut": {f"{m} | {t}": round(v, 5) for (m, t), v in ml.items()},
            "gan_co_lo": _records(r["batch_level"]), "khi_mat_cam_bien": r["outage"],
            "loi_gia_lap": _records(faults[["kich_ban", "phuong_phap", "ti_le_bat_phut_loi", "ti_le_bat_lo_loi"]]),
            "mo_phong": r["simulation"],
            "lo_can_kiem_tra_top5": _records(lst[lst["can_kiem_tra"]].head(5)[show])}

def dien_du_lieu_thieu(minutes: pd.DataFrame, out: Path, models: Path, figs: bool) -> dict:
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
    # dải 95 %, cách điền từng cảm biến, lô phải giữ khi mất UHT
    coverage = {name: band_coverage(r["Z_valid"], np.where(mk, np.nan, r["Z_valid"]), em.mu, em.sigma)
                for name, mk in r["masks_valid"].items() if mk.any()}
    methods = choose_fill_method(r["rmse_z"])
    _csv(pd.DataFrame(methods).T.rename_axis("bien").reset_index(), out / "em_cach_dien_tung_cam_bien.csv")
    j = cols.index(cfg.EM_HOLD_SENSOR)
    lost = pd.Series(r["masks_valid"]["valid_mat_tin_hieu"][:, j]).groupby(r["valid"]["batch_key"].to_numpy()).sum()
    actions = pd.DataFrame({"batch_key": lost.index, "phut_mat_uht": lost.to_numpy().astype(int)})
    actions["hanh_dong"] = [outage_action(cfg.EM_HOLD_SENSOR, int(v))["action"] if v > 0 else "KHONG_MAT_UHT"
                            for v in actions["phut_mat_uht"]]
    _csv(actions, out / "em_hanh_dong_khi_mat_uht.csv")
    hold = {"so_lo_giu": int((actions["hanh_dong"] == "GIU_LO").sum()), "so_lo_valid": int(len(actions)),
            "so_lo_mat_uht": int((actions["phut_mat_uht"] > 0).sum()),
            "quy_tac": f"mất {cfg.EM_HOLD_SENSOR} > {cfg.EM_HOLD_MAX_MINUTES} phút thì giữ lô"}
    # 2) bàn giao: học lại µ, Σ trên toàn bộ dữ liệu phút
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
        "variables": cols, "data": "trung bình theo phút từng lô, G fact_sensor_telemetry, 01–06/2025",
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
            "param_error_vs_full_train": r["param_error"],
            "band95_coverage": coverage},
        "fill_method": methods, "hold_rule": hold,
        "usage": "statistical_models.em_missing.conditional_impute(X, mu, sigma): "
                 "x_m = µ_m + Σ_mo Σ_oo⁻¹ (x_o − µ_o), dùng µ, Σ đơn vị gốc ở trên.",
    }, models / "em_sensor_mvn.json")
    if figs:
        F.fig_em_loglik(em.history, out / "figures")
        F.fig_em_rmse(r["rmse_z"], out / "figures")
    return {"chia": r["split"], **{k: {m: round(v, 4) if isinstance(v, float) else v for m, v in t.items()}
                                   for k, t in total.items()},
            "n_iter": em.n_iter, "loglik_khong_giam": r["loglik_monotone"],
            "dai_95_chua_gia_tri_that": {k: round(v, 4) for k, v in coverage.items()},
            "cam_bien_nen_noi_suy_khi_co_tin_hieu_lai": sorted(c for c, v in methods.items()
                                                               if v["sau_khi_co_tin_hieu_lai"] == "noi_suy"),
            "giu_lo_khi_mat_uht": hold}

def run(sensor: pd.DataFrame, batches: pd.DataFrame, out: Path, models: Path, figs: bool = True,
        tasks: tuple[int, ...] = (1, 2)) -> dict:
    out.mkdir(parents=True, exist_ok=True)
    (out / "figures").mkdir(exist_ok=True)
    models.mkdir(parents=True, exist_ok=True)
    t0 = time.perf_counter()
    # chạy một phần (--tasks) thì giữ kết quả bài còn lại
    old = out / "summary.json"
    keep = ("bai1_giam_sat", "bai2_dien_du_lieu_thieu", "so_phut", "so_lo")
    res = {k: v for k, v in (json.loads(old.read_text(encoding="utf-8")) if old.exists() else {}).items() if k in keep}
    minutes = data.minute_means(sensor)
    res["so_phut"], res["so_lo"] = int(len(minutes)), int(minutes["batch_key"].nunique())
    if 1 in tasks:
        res["bai1_giam_sat"] = giam_sat(minutes, data.batch_scope(batches), out, models, figs)
    if 2 in tasks:
        res["bai2_dien_du_lieu_thieu"] = dien_du_lieu_thieu(minutes, out, models, figs)
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
    ap.add_argument("--tasks", default="1,2", help="chạy một phần: 1 giám sát (lô cần kiểm tra), 2 điền dữ liệu cảm biến bị mất (EM)")
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