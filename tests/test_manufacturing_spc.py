"""Kiểm thử phân hệ 5 (TV5). Chạy: pytest tests/test_manufacturing_spc.py (hoặc python file này)."""

import sys
from pathlib import Path

import numpy as np
import pandas as pd

# tìm gói manufacturing_spc dù nó nằm trong src/ hay cạnh thư mục tests/
SRC = Path(__file__).resolve().parents[1] / "src"
for p in (SRC, Path(__file__).resolve().parents[1]):
    if str(p) not in sys.path:
        sys.path.insert(0, str(p))

from manufacturing_spc.capability import capability  # noqa: E402
from manufacturing_spc.config import Spec  # noqa: E402
from manufacturing_spc.detection_test import detection_montecarlo, detection_on_batches  # noqa: E402
from manufacturing_spc.eda import eda_summary  # noqa: E402
from manufacturing_spc.feature_screening import benjamini_hochberg, screen_features, vif  # noqa: E402
from manufacturing_spc.nelson_rules import compare_with_expected, expected_false_alarm_rates, nelson_flags  # noqa: E402
from manufacturing_spc.spc_charts import (  # noqa: E402
    XbarSLimits, batch_means, c4, cusum, ewma, imr_limits, make_subgroups, phase1_limits, xbar_s_constants)
from manufacturing_spc.synthetic import make_demo_batches, make_demo_sensor  # noqa: E402
from manufacturing_spc.weather_analysis import heat_stress_tests, thi_correlations  # noqa: E402
from manufacturing_spc.run_spc import filter_scope, run  # noqa: E402


def _flag(seq, rule):
    # chuẩn hóa sẵn: tâm 0, sigma 1 cho dễ viết chuỗi thử
    f = nelson_flags(np.array(seq, float), 0.0, 1.0)
    return f[f"rule_{rule}"].to_numpy()


def test_constants_match_textbook_tables():
    k = xbar_s_constants(5)  # Montgomery: c4=0,9400; A3=1,427; B3=0; B4=2,089
    assert abs(k["c4"] - 0.9400) < 1e-3 and abs(k["A3"] - 1.427) < 1e-3
    assert k["B3"] == 0.0 and abs(k["B4"] - 2.089) < 1e-3
    assert abs(c4(10) - 0.9727) < 1e-3 and abs(xbar_s_constants(10)["B3"] - 0.284) < 1e-3


def test_nelson_rule1_to_rule4():
    assert _flag([0, 0, 3.5, 0], 1).tolist() == [False, False, True, False]
    r2 = _flag([0.5] * 9, 2)
    assert r2.tolist() == [False] * 8 + [True]   # đủ 9 điểm mới báo
    assert _flag([0.5] * 8, 2).sum() == 0        # 8 điểm thì chưa
    r3 = _flag([0, 1, 2, 3, 4, 5], 3)            # 6 điểm tăng đều
    assert r3[-1] and r3.sum() == 1
    assert _flag([0, 1, 2, 3, 4], 3).sum() == 0  # 5 điểm: chưa đủ
    alt = [(-1) ** i * 0.5 for i in range(14)]   # 14 điểm xen kẽ
    assert _flag(alt, 4)[-1] and _flag(alt[:13], 4).sum() == 0


def test_nelson_rule5_to_rule8():
    assert _flag([2.5, 0, 2.5], 5)[-1] and not _flag([2.5, 0, -2.5], 5)[-1]  # hai điểm phải cùng phía
    assert _flag([1.5, 1.5, 0, 1.5, 1.5], 6)[-1]
    assert not _flag([1.5, -1.5, 0, 1.5, 1.5], 6)[-1]
    assert _flag([0.1] * 15, 7)[-1] and _flag([0.1] * 14, 7).sum() == 0
    mix = [1.5, -1.5] * 4  # 8 điểm ngoài ±1 sigma, hai phía
    assert _flag(mix, 8)[-1] and _flag(mix[:7], 8).sum() == 0


def test_false_alarm_rate_rule1_iid_normal():
    x = np.random.default_rng(1).normal(0, 1, 200_000)
    rate = _flag(x, 1).mean()
    assert 0.0022 < rate < 0.0032  # lý thuyết 0,0027 (ARL0 ≈ 370)


def test_capability_known_values():
    rng = np.random.default_rng(0)
    x = rng.normal(140, 0.5, 200_000)
    c = capability(x, Spec("t", 137, 143, 140))
    assert abs(c["Cp"] - 2.0) < 0.03 and abs(c["Cpk"] - 2.0) < 0.03 and abs(c["Cpm"] - 2.0) < 0.03
    off = capability(rng.normal(141, 0.5, 200_000), Spec("t", 137, 143, 140))  # trung bình lệch 1 đơn vị
    assert abs(off["Cp"] - 2.0) < 0.03 and abs(off["Cpk"] - 2 / 1.5) < 0.03   # (143−141)/(3·0,5)
    assert abs(off["Cpm"] - 1 / 1.25**0.5) < 0.03                             # 6/(6·√(0,25+1))
    assert off["Cpk"] < off["Cp"] and off["Cpm"] < off["Cpk"]


def test_spec_choice_changes_conclusion():
    """Cùng một quy trình: giới hạn hợp đồng GX (135–145) cho kết luận ngược với giới hạn công nghệ (137–142)."""
    x = np.random.default_rng(2).normal(140, 0.8, 100_000)
    wide = capability(x, Spec("uht", 135, 145))
    tech = capability(x, Spec("uht", 137, 142, 139.5))
    assert wide["Cpk"] > 1.33 > tech["Cpk"]


def test_cusum_and_ewma_detect_small_shift_faster_than_shewhart():
    rng = np.random.default_rng(3)
    x = np.r_[rng.normal(0, 1, 200), rng.normal(1.0, 1, 200)]  # lệch 1 sigma từ điểm 200
    cu = cusum(x, 0.0, 1.0)
    first_cusum = np.flatnonzero(cu["signal_up"].to_numpy())
    first_cusum = first_cusum[first_cusum >= 200][0]
    first_shewhart = np.flatnonzero(_flag(x, 1))
    first_shewhart = first_shewhart[first_shewhart >= 200]
    assert first_cusum - 200 < 25  # ARL1 lý thuyết khoảng 10 với k=0,5; h=5
    assert len(first_shewhart) == 0 or first_cusum <= first_shewhart[0]
    ew = ewma(x, 0.0, 1.0)
    assert ew["signal"].to_numpy()[200:260].any()
    assert not ew["signal"].to_numpy()[:50].any()


def test_phase1_limits_ignore_later_shift():
    sensor = make_demo_sensor(n_batches=40, shift_from_batch=25)
    sg = make_subgroups(sensor, "uht_temp", n=5, every_s=60)
    assert len(sg) == 40 * 120
    lim = phase1_limits(sg, 5, fraction=0.25)
    assert abs(lim.xbar_cl - 140.0) < 0.1  # baseline chưa bị nhiễm bởi lệch mức ở các lô sau
    assert abs(lim.sigma_within - 0.8) < 0.05
    assert abs(lim.sigma_xbar - 0.8 / 5**0.5) < 0.03


def test_benjamini_hochberg_known_example():
    q = benjamini_hochberg(np.array([0.01, 0.04, 0.03, 0.005]))
    assert np.allclose(q, [0.02, 0.04, 0.04, 0.02])


def test_vif_detects_collinearity_and_screen_drops_it():
    rng = np.random.default_rng(4)
    a, b = rng.normal(size=500), rng.normal(size=500)
    df = pd.DataFrame({"a": a, "b": b, "a_copy": a * 2 + rng.normal(0, 0.01, 500),
                       "const": 1.0, "yield_recovery_pct": a + rng.normal(size=500)})
    v = vif(df, ["a", "b", "a_copy"])
    assert v["a"] > 100 and v["a_copy"] > 100 and v["b"] < 1.5
    res = screen_features(df)
    assert "const" not in res["selected_features"] and "yield_recovery_pct" not in res["selected_features"]
    assert not ({"a", "a_copy"} <= set(res["selected_features"]))  # hai biến trùng nhau không được cùng ở lại
    assert "b" in res["selected_features"]


def test_eda_summary_known_values():
    df = pd.DataFrame({"a": np.random.default_rng(0).normal(10, 2, 5000), "k": 3.0})
    e = eda_summary(df, ["a", "k"], "t").set_index("variable")
    assert abs(e.loc["a", "mean"] - 10) < 0.1 and abs(e.loc["a", "std"] - 2) < 0.1
    assert e.loc["a", "normality_p"] > 0.01 and not e.loc["a", "is_constant"]
    assert e.loc["k", "is_constant"] and np.isnan(e.loc["k", "skewness"])


def _demo_limits() -> XbarSLimits:
    # giới hạn giống dữ liệu UHT thật: trung bình 140, sigma_within 0,8, nhóm con n=5
    k = xbar_s_constants(5)
    sw = 0.8
    sbar = sw * k["c4"]
    return XbarSLimits(n=5, xbar_cl=140.0, xbar_ucl=140 + k["A3"] * sbar, xbar_lcl=140 - k["A3"] * sbar,
                       s_cl=sbar, s_ucl=k["B4"] * sbar, s_lcl=k["B3"] * sbar,
                       sigma_within=sw, sigma_xbar=sw / 5**0.5, n_subgroups_used=100)


def test_detection_scenarios_behave_as_theory_says():
    d = detection_montecarlo(_demo_limits(), n_rep=120, horizon=300, inject_at=100, seed=1)
    d = d.set_index(["scenario", "method"])

    # lệch 1 sigma: CUSUM và EWMA phát hiện nhanh hơn Shewhart 3 sigma rõ rệt
    shew = d.loc[("lech_muc_1.0sigma", "shewhart_3sigma"), "mean_delay_subgroups"]
    assert d.loc[("lech_muc_1.0sigma", "cusum"), "mean_delay_subgroups"] < shew
    assert d.loc[("lech_muc_1.0sigma", "ewma"), "mean_delay_subgroups"] < shew

    # đột biến một điểm 4 sigma: Shewhart bắt kịp thời (lý thuyết ≈ 0,84), CUSUM/EWMA thì bỏ lỡ phần lớn
    assert d.loc[("dot_bien_4sigma", "shewhart_3sigma"), "detect_within_10_rate"] > 0.7
    assert d.loc[("dot_bien_4sigma", "cusum"), "detect_within_10_rate"] < 0.55
    assert d.loc[("dot_bien_4sigma", "ewma"), "detect_within_10_rate"] < 0.55

    # tăng phân tán 1,5 lần: biểu đồ S bắt được, còn X̄ chỉ phản ứng yếu
    assert d.loc[("tang_phan_tan_1.5x", "s_chart"), "detect_rate"] > 0.95

    # không bất thường: báo động giả của Shewhart 3 sigma trong 100 nhóm đầu ≈ 1−exp(−100/370) ≈ 0,24
    fa = d.loc[("khong_bat_thuong", "shewhart_3sigma"), "false_alarm_before_rate"]
    assert 0.10 < fa < 0.40
    # áp cả 8 quy tắc Nelson lên một biểu đồ làm báo động giả cao hơn hẳn
    assert d.loc[("khong_bat_thuong", "shewhart_nelson"), "false_alarm_before_rate"] > fa + 0.3


def test_spec_matches_task_document():
    from manufacturing_spc import config as cfg
    sp = cfg.SPECS["uht_temp"]
    assert (sp.lsl, sp.usl, sp.mid) == (137.0, 142.0, 140.0)   # LSL 137, USL 142, Target 140
    assert cfg.SUBGROUP_SIZE == 60 and cfg.SUBGROUP_EVERY_S == 60  # nhóm con 1 phút, n = 60
    assert cfg.PHASE1_MONTHS == (1,) and cfg.PHASE2_MONTHS == (2, 3, 4, 5, 6)
    assert cfg.CUSUM_K == 0.5 and cfg.CUSUM_H == 5.0 and cfg.EWMA_LAMBDA == 0.2 and cfg.EWMA_L == 3.0


def test_imr_limits_known_values():
    x = np.random.default_rng(5).normal(140, 0.2, 20_000)
    im = imr_limits(x)
    assert abs(im.x_cl - 140) < 0.01 and abs(im.sigma - 0.2) < 0.01        # MR̄/d2 ≈ sigma
    assert abs(im.x_ucl - (im.x_cl + 3 * im.sigma)) < 1e-9
    assert abs(im.mr_ucl - 3.267 * im.mr_cl) < 1e-9 and im.mr_lcl == 0.0


def test_expected_false_alarm_rates_match_theory():
    r = expected_false_alarm_rates(n_sim=600_000, seed=3)
    assert 0.0024 < r[1] < 0.0030           # lý thuyết 0,0027
    assert 0.0024 < r[3] < 0.0032           # 2/6! ≈ 0,0028
    assert 0.0028 < r[7] < 0.0038           # 0,6827^15 ≈ 0,0033
    assert all(v > 0 for v in r.values())
    flags = nelson_flags(np.random.default_rng(1).normal(0, 1, 5000), 0.0, 1.0)
    cmp = compare_with_expected(flags, r, "t", "I")
    assert len(cmp) == 8 and (cmp["n_points"] == 5000).all()


def test_batch_detection_catches_big_shift_and_drift():
    # Một chuỗi duy nhất nên chỉ kiểm tra những gì chắc chắn: lệch lớn và trôi dần phải bị bắt.
    # Lệch +0,5σ trên một chuỗi 90 lô có thể không bị bắt (CUSUM cần ≈ 38 lô trung bình), đó là lý do có
    # thêm detection_montecarlo.csv để có thống kê ổn định hơn.
    rng = np.random.default_rng(6)
    n = 110
    bm = pd.DataFrame({"batch_key": np.arange(1, n + 1), "mean": rng.normal(140, 0.2, n)})
    im = imr_limits(rng.normal(140, 0.2, 124))
    d = detection_on_batches(bm, im, inject_at=20).set_index(["scenario", "method"])
    for m in ("shewhart_I", "shewhart_nelson", "cusum", "ewma"):
        assert d.loc[("dich_+1.5sigma", m), "detected"], m
        assert d.loc[("troi_dan_drift", m), "detected"], m
    assert d.loc[("dich_+1.5sigma", "cusum"), "delay_batches"] <= 10
    assert d.loc[("dich_+1.5sigma", "cusum"), "delay_batches"] < d.loc[("dich_+1.5sigma", "shewhart_I"), "delay_batches"]
    # lô phát hiện đầu tiên không được nằm trước lô bắt đầu cấy
    det = d[d["detected"]]
    assert (det["first_detect_batch_id"] >= det["inject_batch_id"]).all()
    # đối chứng không cấy: Shewhart I không được báo động giả sớm trong đoạn trước điểm cấy
    assert not d.loc[("doi_chung_khong_cay", "shewhart_I"), "false_alarm_before_injection"]


def test_thi_tests_run_and_fisher_table_is_consistent():
    b = make_demo_batches(n_batches=240, seed=11)
    c = thi_correlations(b)
    assert set(c["variable"]) == {"fat_in", "protein_in", "yield_recovery_pct"}
    t = heat_stress_tests(b)
    assert {"Mann-Whitney U", "Fisher exact"} <= set(t["test"])
    fisher = t[t["test"] == "Fisher exact"].iloc[0]
    assert fisher["n_group0"] + fisher["n_group1"] == 240


def test_scope_filter_and_full_pipeline_on_small_demo():
    import tempfile
    # 90 lô ≈ 22 ngày nên chỉ có tháng 01; thử ép Phase II bằng 200 lô (có tháng 02) trong test tích hợp ngắn
    sensor = make_demo_sensor(n_batches=200, samples=600, shift_from_batch=150)
    batches = make_demo_batches(n_batches=200, seed=2, sensor=sensor)
    s2026 = sensor.head(100).assign(timestamp=lambda d: d["timestamp"] + pd.DateOffset(years=1), batch_key=9999)
    s, b = filter_scope(pd.concat([sensor, s2026]), batches)
    assert (s["timestamp"].dt.year == 2025).all() and 9999 not in set(s["batch_key"])
    with tempfile.TemporaryDirectory() as tmp:
        res = run(s, b, Path(tmp), make_figs=False)
        for name in ("eda_summary.csv", "spc_limits.json", "nelson_violations.csv", "capability.csv",
                     "correlation_spearman.csv", "correlation_pearson.csv", "vif.csv",
                     "selected_features.json", "detection_test.csv", "nelson_summary.csv"):
            assert (Path(tmp) / name).exists(), name
        assert res["lo_phase"]["I"] > 0 and res["lo_phase"]["II"] > 0


if __name__ == "__main__":
    fails = 0
    for name, fn in list(globals().items()):
        if name.startswith("test_") and callable(fn):
            try:
                fn()
                print("PASS", name)
            except Exception as e:  # noqa: BLE001
                fails += 1
                print("FAIL", name, "->", repr(e))
    sys.exit(1 if fails else 0)
