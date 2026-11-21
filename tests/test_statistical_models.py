"""Chạy: pytest tests/test_statistical_models.py"""

import json
import sys
from pathlib import Path

import numpy as np
import pandas as pd
import pytest

from scipy.special import expit

SRC = Path(__file__).resolve().parents[1] / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

sm = pytest.importorskip("statsmodels.api")

from statistical_models import config as cfg  # noqa: E402
from statistical_models import data  # noqa: E402
from statistical_models.em_missing import (  # noqa: E402
    conditional_impute, em_mvn, fit_final, interpolate_in_batch, mcar_mask, mean_impute, observed_loglik,
    outage_mask, run_em_experiment)
from statistical_models.logistic import (  # noqa: E402
    cluster_cov, export_defect_pipeline, hosmer_lemeshow, irls, log_likelihood, roc_youden)
from statistical_models.run_models import run  # noqa: E402

def _fake_gold(n_groups=40, copies=3, minutes=6, seed=0):
    """Giống cấu trúc Gold thật: mỗi lô MES bị nhân bản `copies` lần (cùng 5 giá trị MES và cùng thống kê cảm biến)."""
    rng = np.random.default_rng(seed)
    rows, sensor, key = [], [], 0
    for g in range(n_groups):
        defect = int(g % 8 == 0)
        mes = {"fat_in": rng.normal(3.5, .1), "protein_in": rng.normal(3.2, .05), "acidity_sh": rng.normal(6.5, .2),
               "yield_recovery_pct": rng.normal(88 if defect else 95, 2), "batch_defect_flag": defect}
        proc = {"uht_temp_std": rng.normal(.8, .01), "uht_temp_min": rng.normal(137 + 0.2 * defect, .25),
                "uht_temp_max": rng.normal(143, .25), "flow_rate_avg": rng.normal(5000 + 0.5 * defect, 1.0),
                "n_anomalies": rng.integers(160, 240), "n_readings": 7200.0}
        drift = rng.normal(0, .1)
        for c in range(copies):
            key += 1
            day = 1 + (key % 28)
            rows.append({"batch_key": key, "batch_id": f"B{key}", "date_key": 20250100 + day, **mes, **proc,
                         "day_of_week": day % 7 + 1, "humidity_avg_pct": rng.normal(80, 9),
                         "thi_index": rng.normal(68, 7), "is_heat_stress": int(rng.random() < .2),
                         "gdt_smp_usd": np.nan if day < 3 else 2700 + 50 * (day > 14), "fao_dairy_index": np.nan})
            n = minutes * 60
            t0 = pd.Timestamp(2025, 1, day, tz="UTC") + pd.Timedelta(hours=c * 2 + g % 5)
            sensor.append(pd.DataFrame({
                "batch_key": key, "timestamp": t0 + pd.to_timedelta(np.arange(n), unit="s"),
                "uht_temp": rng.normal(140 + drift, .8, n), "preheat_temp": rng.normal(80 + drift, .5, n),
                "homo_press_stage1": rng.normal(180 + drift, 3, n), "homo_press_stage2": rng.normal(40 + drift, 1, n),
                "flow_rate": rng.normal(5000 + 10 * drift, 80, n), "conductivity": rng.normal(4.8 + .1 * drift, .1, n),
                "power_kw": rng.normal(75 + drift, 2, n)}))
    b = pd.DataFrame(rows)
    s = pd.concat(sensor, ignore_index=True)
    b = b.merge(s.groupby("batch_key")["homo_press_stage1"].mean().rename("homo_press1_avg").reset_index())
    return s, b

def test_groups_and_dedup_remove_copies():
    _, b = _fake_gold()
    prep = data.prepare_batches(b)
    assert prep["summary"]["n_batches"] == 120 and prep["summary"]["n_groups"] == 40
    assert prep["summary"]["n_defect_groups"] == 5 and prep["summary"]["n_defect_batches"] == 15
    # mỗi nhóm có đúng 3 bản sao, khử trùng giữ dòng nguyên vẹn
    assert prep["all"].groupby(cfg.GROUP_COL).size().eq(3).all()
    assert prep["dedup"]["batch_key"].is_unique
    assert prep["all"]["gdt_smp_usd"].notna().all()  # đã điền giá GDT theo phiên gần nhất

def test_usable_features_drops_nan_and_constant():
    _, b = _fake_gold(n_groups=5)
    keep, dropped = data.usable_features(b, ("protein_in", "fao_dairy_index", "n_readings", "khong_co"))
    assert keep == ["protein_in"]
    assert {d["feature"] for d in dropped} == {"fao_dairy_index", "n_readings", "khong_co"}

def test_group_folds_keep_groups_together_and_reshuffle():
    g = np.repeat(np.arange(30), 4)
    a, b = data.group_folds(g, 5, seed=1), data.group_folds(g, 5, seed=2)
    assert len(a) == 5
    seen = np.concatenate([te for _, te in a])
    assert np.array_equal(np.sort(seen), np.arange(len(g)))          # mỗi dòng được validate đúng một lần
    for tr, te in a:
        assert not set(g[tr]) & set(g[te])                            # bản sao không nằm ở cả train và test
    assert any(not np.array_equal(x[1], y[1]) for x, y in zip(a, b))  # đổi seed là đổi cách chia

def test_split_by_group_has_no_overlap():
    g = np.repeat(np.arange(50), 10)
    tr, va = data.split_by_group(g, 0.2, seed=0)
    assert not set(g[tr]) & set(g[va]) and len(np.unique(g[va])) == 10 and (tr ^ va).all()

def _logit_data(n=600, seed=1):
    rng = np.random.default_rng(seed)
    X = np.column_stack([np.ones(n), rng.normal(size=(n, 3))])
    y = (rng.random(n) < expit(X @ np.array([-1.0, 0.8, -0.5, 0.3]))).astype(float)
    return X, y, rng.integers(0, 60, n)

def test_irls_matches_statsmodels_and_loglik_increases():
    X, y, _ = _logit_data()
    r = irls(X, y)
    ref = sm.Logit(y, X).fit(disp=0)
    assert r.converged and not r.separation_warning
    np.testing.assert_allclose(r.beta, ref.params, atol=1e-8)
    np.testing.assert_allclose(r.se, ref.bse, rtol=1e-6)
    assert abs(r.loglik - ref.llf) < 1e-8
    assert np.all(np.diff(r.history) >= -1e-12)

def test_cluster_cov_matches_statsmodels():
    X, y, g = _logit_data()
    r = irls(X, y)
    ref = sm.Logit(y, X).fit(disp=0, cov_type="cluster", cov_kwds={"groups": g})
    np.testing.assert_allclose(np.sqrt(np.diag(cluster_cov(X, y, r.beta, g))), ref.bse, rtol=1e-6)

def test_irls_flags_complete_separation():
    x = np.r_[np.linspace(-2, -0.1, 20), np.linspace(0.1, 2, 20)]
    X, y = np.column_stack([np.ones(40), x]), (x > 0).astype(float)
    assert irls(X, y, max_iter=50).separation_warning

def test_hosmer_lemeshow_and_youden():
    rng = np.random.default_rng(3)
    p = rng.uniform(0.02, 0.6, 4000)
    y = (rng.random(4000) < p).astype(float)
    hl = hosmer_lemeshow(y, p)
    assert hl["df"] == 8 and hl["p_value"] > 0.001   # mô hình đúng thì không bác bỏ
    bad = hosmer_lemeshow(y, np.clip(p * 0.3, 1e-6, 1))
    assert bad["p_value"] < 1e-6                       # đánh giá thấp xác suất có hệ thống thì bị bác bỏ
    r = roc_youden(np.r_[np.zeros(50), np.ones(50)], np.r_[np.linspace(0, .4, 50), np.linspace(.6, 1, 50)])
    assert r["auc"] == 1.0 and r["sensitivity"] == 1.0 and r["specificity"] == 1.0

def test_defect_pipeline_equals_irls():
    _, b = _fake_gold()
    prep = data.prepare_batches(b)
    feats = list(cfg.DEFECT_FEATURES)
    pipe = export_defect_pipeline(prep["all"], feats)
    d = prep["all"]
    X = d[feats].to_numpy(float)
    Z = (X - X.mean(0)) / X.std(0)
    r = irls(np.column_stack([np.ones(len(Z)), Z]), d[cfg.DEFECT_TARGET].to_numpy(float))
    np.testing.assert_allclose(pipe[-1].coef_[0], r.beta[1:], atol=1e-4)
    np.testing.assert_allclose(pipe.predict_proba(d[feats])[:, 1], expit(np.column_stack([np.ones(len(Z)), Z]) @ r.beta),
                               atol=1e-5)

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
    assert res["chuan_bi"]["n_groups"] == 40
    for f in ("yield_model_real.json", "defect_model_real.json", "em_sensor_mvn.json",
              "yield_model.joblib", "defect_model.joblib"):
        assert (tmp_path / "models" / f).exists(), f
    y = json.loads((tmp_path / "models" / "yield_model_real.json").read_text(encoding="utf-8"))
    assert "fao_dairy_index" not in y["features"] and y["ols_independent"]["n"] == 40
    d = json.loads((tmp_path / "models" / "defect_model_real.json").read_text(encoding="utf-8"))
    assert d["irls_vs_statsmodels_max_abs_diff"]["beta"] < 1e-6
    assert d["auc_oof_repeated"]["n_repeats"] == cfg.CV_REPEATS
    assert {"mse_in_sample", "mse_cv_nested_groupkfold", "r2_cv_sd"} <= set(y["elasticnet"])
    cvs = pd.read_csv(tmp_path / "reports" / "yield_cv_scores.csv")
    assert {"mse_cv", "mse_cv_sd", "r2_cv_sd"} <= set(cvs.columns) and (cvs["n_repeats"] == cfg.CV_REPEATS).all()
    e = json.loads((tmp_path / "models" / "em_sensor_mvn.json").read_text(encoding="utf-8"))
    assert e["validation"]["split"]["batch_overlap"] == 0
    assert e["final_fit"]["rows"] == res["so_phut"]                    # µ, Σ bàn giao học trên toàn bộ dữ liệu
    assert res["bai3_em"]["loglik_khong_giam"] and "bai4_gmm" not in res
    assert len(list((tmp_path / "reports" / "figures").glob("*.png"))) == 7
    # bản sao của một lô không bao giờ nằm ở cả train và test
    folds = pd.read_csv(tmp_path / "reports" / "defect_cv_folds.csv")
    assert (folds["defects_test"] > 0).all()

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
