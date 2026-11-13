"""Pipeline của TV5: EDA, SPC (X̄–S theo phút, I–MR theo lô), CUSUM, EWMA, Nelson, năng lực quy trình,
sàng lọc biến, phân tích THI, kiểm thử phát hiện; ghi file bàn giao và hình vào --out (mặc định reports/spc).

    python -m manufacturing_spc.run_spc --source synthetic --out reports/spc_demo      # chạy thử
    python -m manufacturing_spc.run_spc --source parquet `
        --sensor-path fact_sensor_telemetry.parquet --features-path batch_features.parquet   # dữ liệu thật, không cần Docker
    python -m manufacturing_spc.run_spc --source chdb                                  # cần Docker/chDB
"""

from __future__ import annotations

import argparse
import json
from pathlib import Path

import numpy as np
import pandas as pd

from . import config as cfg
from . import figures
from .capability import capability
from .detection_test import detection_montecarlo, detection_on_batches
from .eda import eda_summary
from .feature_screening import candidate_columns, correlation_matrix, screen_features, target_correlations
from .nelson_rules import compare_with_expected, expected_false_alarm_rates, nelson_flags, violation_log
from .spc_charts import batch_means, cusum, ewma, imr_limits, make_subgroups, phase1_limits
from .weather_analysis import ensure_heat_stress, heat_stress_tests, thi_correlations


def _csv(df: pd.DataFrame, path: Path, index: bool = False) -> None:
    # utf-8-sig để mở bằng Excel không bị lỗi font tiếng Việt
    df.to_csv(path, index=index, encoding="utf-8-sig")


def filter_scope(sensor: pd.DataFrame, batches: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Chỉ giữ dữ liệu năm YEAR có batch_key (theo tài liệu: 240 lô năm 2025)."""
    s = sensor[sensor["batch_key"].notna()].copy()
    s["timestamp"] = pd.to_datetime(s["timestamp"])
    s = s[s["timestamp"].dt.year == cfg.YEAR]
    s["batch_key"] = s["batch_key"].astype("int64")
    b = batches[batches["batch_key"].isin(s["batch_key"].unique())].copy()
    return s, b


def _add_phase(df: pd.DataFrame) -> pd.DataFrame:
    """Gắn nhãn Phase I/II theo tháng của `timestamp`; bỏ các điểm ngoài phạm vi hai pha."""
    df = df.copy()
    df["month"] = pd.to_datetime(df["timestamp"]).dt.month
    df["phase"] = np.where(df["month"].isin(cfg.PHASE1_MONTHS), "I",
                           np.where(df["month"].isin(cfg.PHASE2_MONTHS), "II", "ngoài phạm vi"))
    return df[df["phase"] != "ngoài phạm vi"].reset_index(drop=True)


def _flags_by_phase(x: np.ndarray, phase: np.ndarray, cl: float, sigma: float) -> pd.DataFrame:
    """Gắn cờ Nelson riêng cho từng pha để mẫu hình không bị nối qua ranh giới giữa hai pha."""
    parts = []
    for ph in ("I", "II"):
        idx = np.flatnonzero(phase == ph)
        if len(idx):
            f = nelson_flags(x[idx], cl, sigma)
            f.index = idx
            parts.append(f)
    return pd.concat(parts).sort_index()


def run(sensor: pd.DataFrame, batches: pd.DataFrame, out_dir: Path, make_figs: bool = True) -> dict:
    out_dir.mkdir(parents=True, exist_ok=True)
    fig_dir = out_dir / "figures"
    fig_dir.mkdir(exist_ok=True)
    var, spec = "uht_temp", cfg.SPECS["uht_temp"]

    sensor, batches = filter_scope(sensor, batches)
    batches = ensure_heat_stress(batches)

    # --- X̄–S theo phút (n = 60): giới hạn ước lượng ở Phase I, giám sát cả hai pha ---
    sg = _add_phase(make_subgroups(sensor, value=var, n=cfg.SUBGROUP_SIZE, every_s=cfg.SUBGROUP_EVERY_S))
    lim = phase1_limits(sg, cfg.SUBGROUP_SIZE, phase1_mask=(sg["phase"] == "I").to_numpy())
    ph_sg = sg["phase"].to_numpy()
    flags_x = _flags_by_phase(sg["xbar"].to_numpy(), ph_sg, lim.xbar_cl, lim.sigma_xbar)

    # --- I–MR trên trung bình từng lô (240 điểm) ---
    bm = _add_phase(batch_means(sensor, var))
    imr = imr_limits(bm.loc[bm["phase"] == "I", "mean"].to_numpy(), cfg.D2_N2, cfg.D4_N2)
    ph_bm = bm["phase"].to_numpy()
    flags_i = _flags_by_phase(bm["mean"].to_numpy(), ph_bm, imr.x_cl, imr.sigma)

    # --- CUSUM và EWMA trên trung bình từng lô, dùng tâm và sigma của I–MR Phase I ---
    cu = cusum(bm["mean"].to_numpy(), imr.x_cl, imr.sigma, cfg.CUSUM_K, cfg.CUSUM_H)
    ew = ewma(bm["mean"].to_numpy(), imr.x_cl, imr.sigma, cfg.EWMA_LAMBDA, cfg.EWMA_L)

    # --- Nhật ký Nelson và so với báo động giả kỳ vọng ---
    sgl = sg.rename(columns={"subgroup_id": "point_id"})
    bml = bm.rename(columns={"batch_order": "point_id"})
    log_x = violation_log(sgl, flags_x, "xbar", "xbar_minute", "point_id")
    log_i = violation_log(bml, flags_i, "mean", "I_batch", "point_id")
    log_x["phase"] = log_x["point_id"].map(dict(zip(sgl["point_id"], sgl["phase"])))
    log_i["phase"] = log_i["point_id"].map(dict(zip(bml["point_id"], bml["phase"])))
    log = pd.concat([log_x, log_i], ignore_index=True)
    expected = expected_false_alarm_rates()
    nelson_sum = pd.concat([
        compare_with_expected(flags_x[ph_sg == ph], expected, "xbar_minute", ph) for ph in ("I", "II")
    ] + [
        compare_with_expected(flags_i[ph_bm == ph], expected, "I_batch", ph) for ph in ("I", "II")
    ], ignore_index=True)

    # --- Năng lực quy trình: toàn bộ 2025, rồi tách Phase I và Phase II ---
    sensor_ph = sensor.assign(month=sensor["timestamp"].dt.month)
    scopes = {"toàn bộ 2025": sensor_ph,
              "Phase I": sensor_ph[sensor_ph["month"].isin(cfg.PHASE1_MONTHS)],
              "Phase II": sensor_ph[sensor_ph["month"].isin(cfg.PHASE2_MONTHS)]}
    cap_rows = [{"scope": k, **capability(v[var].to_numpy(), spec, sigma_within=lim.sigma_within)}
                for k, v in scopes.items() if len(v) > 10]
    cap_all = cap_rows[0]

    # --- Sàng lọc biến, tương quan Pearson + Spearman, VIF ---
    cols = candidate_columns(batches)
    corr_s, pval_s = correlation_matrix(batches, cols, "spearman")
    corr_p, pval_p = correlation_matrix(batches, cols, "pearson")
    tcorr = target_correlations(batches, cols)
    screen = screen_features(batches)
    dropped = {d["feature"]: d["reason"] for d in screen["dropped"]}
    vif_tbl = pd.DataFrame([{
        "feature": f, "vif_initial": screen["vif_initial"].get(f), "vif_final": screen["vif_final"].get(f),
        "status": "selected" if f in screen["selected_features"] else "dropped",
        "reason": dropped.get(f, "")} for f in sorted(set(screen["vif_initial"]) | set(dropped))])

    # --- EDA ---
    sensor_cols = [c for c in sensor.select_dtypes("number").columns if c not in cfg.NON_FEATURE_COLUMNS]
    eda = pd.concat([eda_summary(sensor, sensor_cols, "sensor_1hz"),
                     eda_summary(batches, cols + [t for t in cfg.TARGETS if t in batches.columns], "batch")],
                    ignore_index=True)

    # --- THI ---
    thi_corr = thi_correlations(batches)
    thi_tests = heat_stress_tests(batches)

    # --- Kiểm thử phát hiện ---
    p2 = bm[bm["phase"] == "II"].reset_index(drop=True)
    detect = (detection_on_batches(p2, imr, inject_at=min(20, max(len(p2) // 3, 1))) if len(p2) > 5 else pd.DataFrame())
    detect_mc = detection_montecarlo(lim)

    # --- Ghi file ---
    p2_start_sg = int(np.argmax(ph_sg == "II")) if (ph_sg == "II").any() else None
    p2_start_bm = int(np.argmax(ph_bm == "II")) if (ph_bm == "II").any() else None
    spc_limits = {
        "variable": var, "scope": f"năm {cfg.YEAR}, chỉ các lô có batch_key",
        "phase1_months": list(cfg.PHASE1_MONTHS), "phase2_months": list(cfg.PHASE2_MONTHS),
        "n_subgroups": {"I": int((ph_sg == "I").sum()), "II": int((ph_sg == "II").sum())},
        "n_batches": {"I": int((ph_bm == "I").sum()), "II": int((ph_bm == "II").sum())},
        "xbar_s": {"subgroup_size": cfg.SUBGROUP_SIZE, "subgroup_every_s": cfg.SUBGROUP_EVERY_S, **lim.to_dict()},
        "imr": imr.to_dict(),
        "cusum": {"on": "batch_mean", "k": cfg.CUSUM_K, "h": cfg.CUSUM_H, "mu0": imr.x_cl, "sigma": imr.sigma},
        "ewma": {"on": "batch_mean", "lambda": cfg.EWMA_LAMBDA, "L": cfg.EWMA_L, "mu0": imr.x_cl, "sigma": imr.sigma},
        "spec": {"LSL": spec.lsl, "USL": spec.usl, "target": spec.mid},
    }
    (out_dir / "spc_limits.json").write_text(json.dumps(spc_limits, indent=2, ensure_ascii=False), encoding="utf-8")
    _csv(eda, out_dir / "eda_summary.csv")
    _csv(log, out_dir / "nelson_violations.csv")
    _csv(nelson_sum, out_dir / "nelson_summary.csv")
    _csv(sg.join(flags_x), out_dir / "spc_series.csv")
    _csv(bm.join(flags_i.add_prefix("i_")).join(cu).join(ew.add_prefix("ewma_")), out_dir / "spc_series_batch.csv")
    _csv(pd.DataFrame(cap_rows), out_dir / "capability.csv")
    _csv(corr_s, out_dir / "correlation_spearman.csv", index=True)
    _csv(pval_s, out_dir / "correlation_spearman_pvalues.csv", index=True)
    _csv(corr_p, out_dir / "correlation_pearson.csv", index=True)
    _csv(pval_p, out_dir / "correlation_pearson_pvalues.csv", index=True)
    _csv(tcorr, out_dir / "correlation_target.csv")
    _csv(thi_corr, out_dir / "correlation_thi.csv")
    _csv(thi_tests, out_dir / "thi_heat_stress_tests.csv")
    _csv(vif_tbl, out_dir / "vif.csv")
    _csv(detect, out_dir / "detection_test.csv")
    _csv(detect_mc, out_dir / "detection_montecarlo.csv")
    (out_dir / "selected_features.json").write_text(json.dumps(screen, indent=2, ensure_ascii=False), encoding="utf-8")
    try:  # TV7 có thể muốn parquet, chỉ ghi khi máy có pyarrow
        log.to_parquet(out_dir / "nelson_violations.parquet", index=False)
    except ImportError:
        pass

    figs: list[str] = []
    if make_figs:
        made = [
            figures.fig_hist_qq(sensor, fig_dir),
            figures.fig_boxplots(batches, fig_dir),
            figures.fig_xbar_s(sg, flags_x, lim, p2_start_sg, fig_dir),
            figures.fig_imr(bm, flags_i, imr, p2_start_bm, fig_dir),
            figures.fig_cusum(cu, cfg.CUSUM_H, p2_start_bm, fig_dir),
            figures.fig_ewma(ew, bm, imr.x_cl, p2_start_bm, fig_dir),
            figures.fig_capability(sensor, spec, cap_all, fig_dir),
            figures.fig_corr_heatmap(corr_s, pval_s, fig_dir, "Spearman"),
            figures.fig_corr_heatmap(corr_p, pval_p, fig_dir, "Pearson"),
            figures.fig_thi(batches, thi_corr, fig_dir),
        ]
        figs = [p.name for p in made if p is not None]

    return {
        "so_lo": int(len(bm)), "so_nhom_con": int(len(sg)),
        "nhom_con_phase": {"I": spc_limits["n_subgroups"]["I"], "II": spc_limits["n_subgroups"]["II"]},
        "lo_phase": spc_limits["n_batches"],
        "capability_toan_bo": {k: cap_all[k] for k in ("Cp", "Cpk", "Cpm", "Pp", "Ppk", "conclusion")},
        # đếm điểm duy nhất bị gắn cờ (một điểm vi phạm nhiều quy tắc chỉ tính một lần) và tỉ lệ trên số điểm
        "diem_vi_pham_nelson": {
            f"{c}/pha_{p}": {"so_diem": n, "ty_le": round(n / max(m, 1), 4)}
            for c, fl, ph in (("xbar_minute", flags_x, ph_sg), ("I_batch", flags_i, ph_bm))
            for p in ("I", "II")
            for n, m in [(int(fl["any_violation"].to_numpy()[ph == p].sum()), int((ph == p).sum()))]},
        "cusum_tin_hieu": int((cu["signal_up"] | cu["signal_down"]).sum()), "ewma_tin_hieu": int(ew["signal"].sum()),
        "bien_giu_lai": len(screen["selected_features"]), "hinh_da_ve": figs,
    }


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--source", choices=["chdb", "parquet", "synthetic"], default="synthetic")
    ap.add_argument("--db-path", default=None)
    ap.add_argument("--sensor-path", default="data/03_gold_marts/fact_sensor_telemetry.parquet")
    ap.add_argument("--features-path", default="data/03_gold_marts/batch_features.parquet")
    ap.add_argument("--out", default="reports/spc")
    a = ap.parse_args()

    if a.source == "chdb":
        from .data_access import load_from_chdb
        sensor, batches = load_from_chdb(a.db_path)
    elif a.source == "parquet":
        from .data_access import load_from_parquet
        sensor, batches = load_from_parquet(a.sensor_path, a.features_path)
    else:
        from .synthetic import make_demo_batches, make_demo_sensor
        sensor = make_demo_sensor()
        batches = make_demo_batches(sensor=sensor)
    res = run(sensor, batches, Path(a.out))
    print(json.dumps(res, indent=2, ensure_ascii=False, default=float))
    print(f"\nĐã ghi kết quả và hình vào: {Path(a.out).resolve()}")


if __name__ == "__main__":
    main()
