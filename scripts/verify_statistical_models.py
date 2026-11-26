"""Đối chiếu kết quả điền dữ liệu cảm biến bị mất (EM) và giám sát (Hotelling T²) trên Gold thật với số mong đợi của bản mô tả.

Chạy ở gốc repo (PowerShell), cần dữ liệu ở data/03_gold_marts/:
    $env:PYTHONPATH = "src"
    python scripts/verify_statistical_models.py
"""

import sys
import numpy as np
import pandas as pd

from statistical_models import config as cfg
from statistical_models import data
from statistical_models.em_missing import (band_coverage, choose_fill_method, fit_final, outage_action,
                                           run_em_experiment)
from statistical_models.hotelling import detection_check, run_hotelling

if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8")  # console Windows mặc định cp1252

rows = []


def check(name, value, expected, ok):
    rows.append((name, value, expected, "ĐẠT" if ok else "LỆCH"))

sensor, batches = data.load_gold("data/03_gold_marts/fact_sensor_telemetry.parquet",
                                 "data/03_gold_marts/batch_features.parquet")
minutes = data.minute_means(sensor)
check("Số phút cảm biến", len(minutes), 172800, len(minutes) == 172800)

# Điền dữ liệu cảm biến bị mất (EM)
r = run_em_experiment(minutes)
em, cols = r["em"], r["cols"]
rz = r["rmse_z"].set_index(["tap", "bien"])
e, m = rz.loc[("valid_mat_tin_hieu", "TOAN_BO"), "EM"], rz.loc[("valid_mat_tin_hieu", "TOAN_BO"), "trung_binh"]
check("EM log-likelihood không giảm", r["loglik_monotone"], True, r["loglik_monotone"])
check("RMSE EM, mất tín hiệu 10–15 phút", round(e, 3), "khoảng 0,843", abs(e - 0.843) < 0.01)
check("RMSE điền trung bình, cùng kịch bản", round(m, 3), "khoảng 1,008", abs(m - 1.008) < 0.01)
methods = choose_fill_method(r["rmse_z"])
interp = sorted(c for c, v in methods.items() if v["sau_khi_co_tin_hieu_lai"] == "noi_suy")
check("Cảm biến nên điền bằng nội suy", ", ".join(interp), "flow_rate, power_kw, preheat_temp",
      interp == ["flow_rate", "power_kw", "preheat_temp"])
for name, mk in r["masks_valid"].items():
    cov = band_coverage(r["Z_valid"], np.where(mk, np.nan, r["Z_valid"]), em.mu, em.sigma)
    check(f"Dải 95 % chứa giá trị thật ({name})", round(cov, 3), "khoảng 0,95", 0.93 <= cov <= 0.97)
j = cols.index(cfg.EM_HOLD_SENSOR)
lost = pd.Series(r["masks_valid"]["valid_mat_tin_hieu"][:, j]).groupby(r["valid"]["batch_key"].to_numpy()).sum()
held = f"{int((lost > cfg.EM_HOLD_MAX_MINUTES).sum())}/{len(lost)}"
check("Số lô phải giữ (mất uht_temp > 5 phút)", held, "86/288", held == "86/288")
check("Quy tắc giữ lô khi mất UHT 12 phút", outage_action("uht_temp", 12)["action"], "GIU_LO",
      outage_action("uht_temp", 12)["action"] == "GIU_LO")

# Giám sát dây chuyền (Hotelling T²) với µ, Σ của EM
fin = fit_final(minutes)
h = run_hotelling(minutes, np.asarray(fin["mu"]), np.asarray(fin["sigma"]), list(fin["cols"]), batches)
check("Số phút T² vượt ngưỡng", h["n_signals"], 372, h["n_signals"] == 372)
check("Tỉ lệ báo động (kỳ vọng 0,0027)", round(h["signal_rate"], 4), "khoảng 0,0022",
      abs(h["signal_rate"] - 0.00215) < 0.0005)
chk = detection_check(np.asarray(fin["mu"]), np.asarray(fin["sigma"]), list(fin["cols"]))
check("T² bắt tổ hợp lạ (mô phỏng)", round(chk["t2_detection_rate"], 2), "khoảng 0,66",
      abs(chk["t2_detection_rate"] - 0.66) < 0.03)
check("3 sigma từng biến bắt (mô phỏng)", round(chk["univariate_3sigma_detection_rate"], 2), "khoảng 0,32",
      abs(chk["univariate_3sigma_detection_rate"] - 0.32) < 0.03)

w = max(len(n) for n, *_ in rows)
for n, v, ex, ok in rows:
    print(f"{n:<{w}}  {str(v):<36} mong đợi {str(ex):<34} {ok}")
bad = sum(ok == "LỆCH" for *_, ok in rows)
print(f"\n{len(rows) - bad}/{len(rows)} mục ĐẠT" + ("" if bad == 0 else "  -> xem lại mục LỆCH"))