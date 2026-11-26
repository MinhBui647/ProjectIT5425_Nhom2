"""Chạy: pytest tests/test_statistical_models.py"""

import json
import sys
from pathlib import Path

import numpy as np
import pandas as pd
import pytest

SRC = Path(__file__).resolve().parents[1] / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

from statistical_models import config as cfg  # noqa: E402
from statistical_models import data  # noqa: E402
from statistical_models.em_missing import (  # noqa: E402
    band_coverage, choose_fill_method, conditional_impute, conditional_impute_sd, em_mvn, fit_final,
    interpolate_in_batch, mcar_mask, mean_impute, observed_loglik, outage_action, outage_mask, run_em_experiment)
from statistical_models.hotelling import (  # noqa: E402
    choose_k, detection_check, inject_fault, run_hotelling, run_monitoring, t2_scores, univariate_signal)
from statistical_models.run_models import run  # noqa: E402

def _fake_gold(n_batches=120, minutes=6, seed=0):
    """Gold giả: lô trải trên tháng 01–03 (Phase I/II), mỗi lô có drift chung cho các cảm biến."""
    rng = np.random.default_rng(seed)
    rows, sensor = [], []
    for key in range(1, n_batches + 1):
        day, month = 1 + (key % 28), 1 + (key % 3)
        rows.append({"batch_key": key, "batch_id": f"B{key}", "date_key": 20250000 + 100 * month + day,
                     "line_id": f"LINE_UHT_{1 + key % 2}", "batch_defect_flag": int(key % 8 == 0),
                     "n_readings": 7200.0})
        drift, n = rng.normal(0, .1), minutes * 60
        t0 = pd.Timestamp(2025, month, day, tz="UTC") + pd.Timedelta(hours=key % 8)
        sensor.append(pd.DataFrame({
            "batch_key": key, "timestamp": t0 + pd.to_timedelta(np.arange(n), unit="s"),
            "uht_temp": rng.normal(140 + drift, .8, n), "preheat_temp": rng.normal(80 + drift, .5, n),
            "homo_press_stage1": rng.normal(180 + drift, 3, n), "homo_press_stage2": rng.normal(40 + drift, 1, n),
            "flow_rate": rng.normal(5000 + 10 * drift, 80, n), "conductivity": rng.normal(4.8 + .1 * drift, .1, n),
            "power_kw": rng.normal(75 + drift, 2, n)}))
    return pd.concat(sensor, ignore_index=True), pd.DataFrame(rows)

def test_split_by_group_has_no_overlap():
    g = np.repeat(np.arange(50), 10)
    tr, va = data.split_by_group(g, 0.2, seed=0)
    assert not set(g[tr]) & set(g[va]) and len(np.unique(g[va])) == 10 and (tr ^ va).all()

def _mvn(n=2000, seed=4):
    rng = np.random.default_rng(seed)
    A = rng.normal(size=(5, 5))
    cov = A @ A.T / 5 + np.eye(5) * 0.2
    return rng.multivariate_normal(np.zeros(5), cov, n), cov, rng

def test_em_without_missing_gives_sample_moments():
    X, _, _ = _mvn(300)
    r = em_mvn(X)
    np.testing.assert_allclose(r.mu, X.mean(0), atol=1e-10)
    np.testing.assert_allclose(r.sigma, np.cov(X, rowvar=False, ddof=0), atol=1e-10)

def test_em_beats_mean_imputation_and_loglik_monotone():
    X, cov, rng = _mvn()
    mask = mcar_mask(X.shape, 0.1, rng)
    assert not mask.all(axis=1).any()
    Xn = np.where(mask, np.nan, X)
    r = em_mvn(Xn)
    assert r.converged and np.all(np.diff(r.history) >= -1e-8)
    rmse_em = np.sqrt(np.mean((r.filled[mask] - X[mask]) ** 2))
    rmse_mean = np.sqrt(np.mean((mean_impute(Xn)[mask] - X[mask]) ** 2))
    assert rmse_em < 0.9 * rmse_mean
    np.testing.assert_allclose(r.sigma, cov, atol=0.25)
    # tham số EM cho log-likelihood quan sát được cao hơn tham số của điền trung bình
    mi = mean_impute(Xn)
    ll_mi = observed_loglik(Xn, mask, mi.mean(0), np.cov(mi, rowvar=False, ddof=0))
    assert r.history[-1] > ll_mi

def test_conditional_impute_matches_em_fill():
    X, _, rng = _mvn(800)
    Xn = np.where(mcar_mask(X.shape, 0.15, rng), np.nan, X)
    r = em_mvn(Xn)
    np.testing.assert_allclose(conditional_impute(Xn, r.mu, r.sigma), r.filled, atol=1e-12)
    assert not np.isnan(r.filled).any()

def test_em_experiment_train_valid_split_by_batch():
    rng = np.random.default_rng(8)
    n_batch, per = 100, 30
    drift = np.repeat(rng.normal(0, 1, (n_batch, 1)), per, axis=0)  # drift chung theo lô -> các biến tương quan
    cols = list(cfg.SENSOR_VARS)
    vals = drift + rng.normal(0, 0.7, (n_batch * per, len(cols)))
    minutes = pd.DataFrame(vals, columns=cols).assign(batch_key=np.repeat(np.arange(n_batch), per),
                                                      minute=np.tile(np.arange(per), n_batch))
    r = run_em_experiment(minutes, n_rows=1500)
    assert r["split"]["batch_overlap"] == 0 and r["split"]["n_batches_valid"] == 20
    rz = r["rmse_z"].set_index(["tap", "bien"])
    for scen in ("valid_mcar", "valid_mat_tin_hieu"):
        assert rz.loc[(scen, "TOAN_BO"), "EM"] < 0.8 * rz.loc[(scen, "TOAN_BO"), "trung_binh"]
        assert rz.loc[(scen, "TOAN_BO"), "noi_suy_thoi_gian"] > 0
    assert r["loglik_monotone"]

def test_run_end_to_end(tmp_path):
    s, b = _fake_gold()
    res = run(s, b, tmp_path / "reports", tmp_path / "models", figs=True)
    for f in ("monitoring_t2.json", "em_sensor_mvn.json"):
        assert (tmp_path / "models" / f).exists(), f
    t2 = json.loads((tmp_path / "models" / "monitoring_t2.json").read_text(encoding="utf-8"))
    assert t2["train_phase1"]["months"] == list(cfg.T2_PHASE1_MONTHS) and t2["rule_lo_can_kiem_tra"]["k"] >= 1
    assert t2["train_phase1"]["n_defect_batches_excluded"] > 0                # Phase I chỉ dùng lô đạt
    assert t2["validate_phase2"]["n_batches"] + t2["train_phase1"]["n_batches"] + \
        t2["train_phase1"]["n_defect_batches_excluded"] == res["so_lo"]
    lst = pd.read_csv(tmp_path / "reports" / "lo_can_kiem_tra_phase2.csv")
    assert len(lst) == t2["validate_phase2"]["n_batches"] and "can_kiem_tra" in lst
    e = json.loads((tmp_path / "models" / "em_sensor_mvn.json").read_text(encoding="utf-8"))
    assert e["validation"]["split"]["batch_overlap"] == 0
    assert e["final_fit"]["rows"] == res["so_phut"]                    # µ, Σ bàn giao học trên toàn bộ dữ liệu
    assert set(e["fill_method"]) == set(cfg.SENSOR_VARS) and "hold_rule" in e
    act = pd.read_csv(tmp_path / "reports" / "em_hanh_dong_khi_mat_uht.csv")
    assert len(act) == e["hold_rule"]["so_lo_valid"] == e["validation"]["split"]["n_batches_valid"]
    assert (act["hanh_dong"] == "GIU_LO").sum() == e["hold_rule"]["so_lo_giu"]
    assert (tmp_path / "reports" / "em_cach_dien_tung_cam_bien.csv").exists()
    assert res["bai2_dien_du_lieu_thieu"]["loglik_khong_giam"]
    assert len(list((tmp_path / "reports" / "figures").glob("*.png"))) == 4
    # chạy riêng 1 bài thì giữ kết quả bài kia trong summary.json
    res2 = run(s, b, tmp_path / "reports", tmp_path / "models", figs=False, tasks=(1,))
    assert "bai2_dien_du_lieu_thieu" in res2

def test_t2_matches_chi2_and_contributions_sum():
    X, cov, _ = _mvn(20000, seed=11)
    sc, contrib = t2_scores(X, np.zeros(5), cov, alpha=0.01)
    assert abs(sc["signal"].mean() - 0.01) < 0.003                      # dữ liệu ổn định: báo động ≈ α
    np.testing.assert_allclose(np.nansum(contrib, 1), sc["t2"], atol=1e-9)
    Xn = X.copy()
    Xn[:, 0] = np.nan                                                   # mất 1 cảm biến: tính trên 4 chiều còn lại
    sn, _ = t2_scores(Xn, np.zeros(5), cov, alpha=0.01)
    assert (sn["n_observed"] == 4).all() and abs(sn["signal"].mean() - 0.01) < 0.003
    se, _ = t2_scores(np.full((2, 5), np.nan), np.zeros(5), cov)    # mất hết cảm biến
    assert se["t2"].isna().all() and not se["signal"].any() and (se["n_observed"] == 0).all()

def test_run_hotelling_top_sensor_and_per_batch():
    cols = list("abc")
    X = np.zeros((6, 3))
    X[1, 2] = 10.0                                                       # lô 1, phút 2: cảm biến c lệch mạnh
    minutes = pd.DataFrame(X, columns=cols).assign(batch_key=[1, 1, 1, 2, 2, 2], minute=[0, 1, 2] * 2)
    b = pd.DataFrame({"batch_key": [1, 2], "batch_id": ["B1", "B2"], "line_id": ["L1", "L2"]})
    h = run_hotelling(minutes, np.zeros(3), np.eye(3), cols, b)
    assert h["n_signals"] == 1 and h["top_sensor_counts"] == {"c": 1}
    assert h["minutes"].loc[1, "top_share"] == 1.0 and h["minutes"].loc[1, "batch_id"] == "B1"
    pb = h["per_batch"].set_index("batch_key")
    assert pb.loc[1, "n_signals"] == 1 and pb.loc[2, "n_signals"] == 0 and pb.loc[1, "t2_max"] == 100.0

def test_t2_catches_unusual_combination_better_than_3_sigma():
    _, cov, _ = _mvn(10, seed=12)
    chk = detection_check(np.zeros(5), cov, list("abcde"), n=5000)
    assert chk["t2_detection_rate"] > chk["univariate_3sigma_detection_rate"]
    assert abs(chk["false_alarm_rate"] - cfg.T2_ALPHA) < 0.003

def test_choose_k_and_inject_fault():
    assert choose_k(pd.Series([0] * 90 + [1] * 8 + [2] * 2), max_rate=0.05) == 2
    keys = np.repeat(np.arange(4), 30)
    X = np.zeros((120, 2))
    Xf, fault = inject_fault(X, keys, {1: -2.0}, np.array([1.0, 3.0]), 15, np.random.default_rng(0))
    assert fault.sum() == 60 and (Xf[fault, 1] == -6.0).all() and (Xf[:, 0] == 0).all()
    for k in range(4):                                                   # mỗi lô đúng 1 đoạn liền 15 phút
        idx = np.flatnonzero(fault[keys == k])
        assert len(idx) == 15 and np.all(np.diff(idx) == 1)
    assert univariate_signal(np.array([[0.0, 3.5], [0.0, 1.0]]), np.zeros(2), np.eye(2)).tolist() == [True, False]

def test_run_monitoring_train_phase1_validate_phase2():
    s, b = _fake_gold()
    r = run_monitoring(data.minute_means(s), b)
    assert r["phase1"]["months"] == [1] and r["phase2"]["n_batches"] > 0
    assert r["phase1"]["ti_le_lo_gan_co"] <= cfg.T2_BATCH_MAX_FLAG_RATE  # k chọn trên Phase I
    assert set(r["faults"]["phuong_phap"]) == {"Hotelling T²", "3 sigma từng cảm biến"}
    lst = r["inspection_list"]
    assert (lst["can_kiem_tra"] == (lst["n_signals"] >= r["k_t2"])).all()
    assert (lst.loc[lst["can_kiem_tra"], "ly_do"] != "").all()
    assert r["phase2"]["n_lo_can_kiem_tra"] == lst["can_kiem_tra"].sum()
    assert not set(lst["batch_key"]) & set(b.loc[b["date_key"] // 100 % 100 == 1, "batch_key"])  # chỉ lô Phase II

def test_run_monitoring_needs_phase2():
    s, b = _fake_gold()
    b1 = b[b["date_key"] // 100 % 100 == 1]
    with pytest.raises(ValueError):
        run_monitoring(data.minute_means(s[s["batch_key"].isin(b1["batch_key"])]), b1)

def test_conditional_impute_sd_and_band_coverage():
    X, cov, rng = _mvn(6000, seed=13)
    mask = mcar_mask(X.shape, 0.2, rng)
    Xn = np.where(mask, np.nan, X)
    filled, sd = conditional_impute_sd(Xn, np.zeros(5), cov)
    np.testing.assert_allclose(filled, conditional_impute(Xn, np.zeros(5), cov), atol=1e-12)
    assert (sd[~mask] == 0).all() and (sd[mask] > 0).all()
    assert 0.93 <= band_coverage(X, Xn, np.zeros(5), cov) <= 0.97       # dải 95 % chứa ≈ 95 % giá trị thật
    f_all, sd_all = conditional_impute_sd(np.full((1, 5), np.nan), np.ones(5), cov)  # mất hết: về µ, sd biên
    np.testing.assert_allclose(f_all[0], np.ones(5))
    np.testing.assert_allclose(sd_all[0], np.sqrt(np.diag(cov)))

def test_choose_fill_method_and_outage_action():
    rz = pd.DataFrame({"tap": ["valid_mat_tin_hieu"] * 3, "bien": ["a", "b", "TOAN_BO"],
                       "EM": [0.5, 0.9, 0.7], "noi_suy_thoi_gian": [0.8, 0.2, 0.5], "trung_binh": [1.0, 1.0, 1.0]})
    m = choose_fill_method(rz)
    assert set(m) == {"a", "b"} and m["a"]["sau_khi_co_tin_hieu_lai"] == "EM"
    assert m["b"]["sau_khi_co_tin_hieu_lai"] == "noi_suy" and m["b"]["khi_dang_mat"] == "EM"
    assert outage_action("uht_temp", cfg.EM_HOLD_MAX_MINUTES + 1)["action"] == "GIU_LO"
    assert outage_action("uht_temp", cfg.EM_HOLD_MAX_MINUTES)["action"] == "DIEN_VA_DANH_DAU"
    assert outage_action("flow_rate", 60)["action"] == "DIEN_VA_DANH_DAU"

def test_outage_mask_contiguous_10_to_15_minutes():
    keys = np.repeat(np.arange(12), 120)
    m = outage_mask(keys, 7, np.random.default_rng(0))
    assert not m.all(axis=1).any()
    for k in range(12):
        blk = m[keys == k]
        hit = np.flatnonzero(blk.any(axis=0))
        assert len(hit) == cfg.EM_OUTAGES_PER_BATCH                    # đúng 2 cảm biến mất tín hiệu mỗi lô
        for c in hit:
            idx = np.flatnonzero(blk[:, c])
            assert cfg.EM_OUTAGE_MIN <= len(idx) <= cfg.EM_OUTAGE_MAX
            assert np.all(np.diff(idx) == 1)                           # một đoạn liền nhau

def test_interpolate_in_batch_stays_inside_batch():
    X = np.array([[0.0], [np.nan], [2.0], [10.0], [np.nan], [np.nan]])
    out = interpolate_in_batch(X, np.array([1, 1, 1, 2, 2, 2]))
    assert out[1, 0] == 1.0 and out[4, 0] == 10.0 and out[5, 0] == 10.0   # không lấy giá trị của lô khác

def test_fit_final_on_complete_data_is_sample_moments():
    X, _, _ = _mvn(400)
    df = pd.DataFrame(X, columns=list("abcde"))
    f = fit_final(df, cols=tuple("abcde"))
    np.testing.assert_allclose(f["mu"], X.mean(0), atol=1e-10)
    np.testing.assert_allclose(f["sigma"], np.cov(X, rowvar=False, ddof=0), atol=1e-10)
