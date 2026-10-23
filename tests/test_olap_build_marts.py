from datetime import date, datetime, timezone

import polars as pl
import pytest

from olap_engineering import build_marts as bm

UTC = timezone.utc


def _sensor() -> pl.LazyFrame:
    ts = [datetime(2026, 1, 15, 1, 0, s, tzinfo=UTC) for s in range(4)]  # 08:00 ICT
    return pl.LazyFrame({
        "timestamp": ts,
        "line_id": ["L1"] * 4,
        "batch_id": ["B1", "B1", "B2", None],
        "preheat_temp": [80.0] * 4,
        "uht_temp": [140.0, 142.0, 139.0, 140.0],
        "homo_press_stage1": [200.0] * 4,
        "homo_press_stage2": [50.0] * 4,
        "flow_rate": [12000.0] * 4,
        "conductivity": [5.0] * 4,
        "power_kw": [3600.0] * 4,
        "is_anomaly_flag": [False, True, False, None],  # set by TV3 outliers.py
    })


def _mes() -> pl.DataFrame:
    start = datetime(2026, 1, 15, 1, 0, tzinfo=UTC)
    end = datetime(2026, 1, 15, 3, 0, tzinfo=UTC)
    return pl.DataFrame({
        "batch_id": ["B1", "B2"],
        "line_id": ["L1", "L1"],
        "production_start": [start, start],
        "production_end": [end, end],
        "sku_id": ["UHT_PURE", "UHT_LOWFAT"],
        "fat_in": [3.5, 3.4],
        "protein_in": [3.2, 3.1],
        "acidity_sh": [6.5, 6.6],
        "yield_recovery_pct": [95.0, 88.0],
        "microbiology_status": ["PASSED", "FAILED"],
        "batch_defect_flag": [0, 1],
    })


@pytest.fixture
def dims():
    sensor, mes = _sensor(), _mes()
    return {
        "dim_line": bm.build_dim_line(sensor, mes),
        "dim_product": bm.build_dim_product(mes),
        "dim_batch": bm.build_dim_batch(mes, sensor),
        "dim_farm": bm.build_dim_farm(date(2024, 1, 1)),
    }


def test_dim_date_keys():
    d = bm.build_dim_date(date(2026, 1, 1), date(2026, 1, 31))
    assert d.height == 31
    assert d["date_key"][0] == 20260101
    assert d["date_key"].n_unique() == 31


def test_dim_time_has_1440_minutes_and_shifts():
    t = bm.build_dim_time()
    assert t.height == 1440
    assert t.filter(pl.col("time_key") == 800)["shift_code"][0] == "MORNING"
    assert t.filter(pl.col("time_key") == 2300)["shift_code"][0] == "NIGHT"


def test_fact_sensor_keeps_all_rows_and_uses_local_time(dims):
    f = bm.build_fact_sensor(_sensor(), dims["dim_line"], dims["dim_batch"]).collect()
    assert f.height == 4
    assert f["line_key"].null_count() == 0
    assert f["batch_key"].null_count() == 1          # row without batch
    assert set(f["time_key"]) == {800}               # 01:00 UTC = 08:00 ICT
    assert set(f["date_key"]) == {20260115}
    assert f.sort("timestamp")["is_anomaly_flag"].to_list() == [0, 1, 0, 0]  # TV3 flag, null -> 0


def test_fact_batch_one_row_per_batch(dims):
    fs = bm.build_fact_sensor(_sensor(), dims["dim_line"], dims["dim_batch"])
    fb = bm.build_fact_batch(_mes(), fs, dims, {"B1": "BA_VI"})
    assert fb.height == 2
    b1 = fb.filter(pl.col("batch_key") == dims["dim_batch"].filter(pl.col("batch_id") == "B1")["batch_key"][0])
    assert b1["n_readings"][0] == 2
    assert b1["uht_temp_avg"][0] == pytest.approx(141.0)
    assert b1["power_kwh"][0] == pytest.approx(2.0)  # 2 s × 3600 kW / 3600
    assert b1["n_anomalies"][0] == 1
    assert b1["farm_key"].is_not_null().all()


def test_require_governed_rejects_silver_not_written_by_tv3():
    with pytest.raises(SystemExit, match="run_governance"):
        bm.require_governed(_sensor(), "sensor_telemetry")


def test_require_governed_keeps_gate_approved_rows():
    lf = _sensor().with_columns(
        _quality_status=pl.Series(["PASS", "PASS_WITH_REJECTIONS", "FAIL", "PASS"]),
        _contract_version=pl.lit("v1"),
    )
    assert bm.require_governed(lf, "sensor_telemetry").collect().height == 3


def test_fact_external_uses_tv3_thi_and_heat_stress(dims):
    hours = [datetime(2025, 1, d, h, tzinfo=UTC) for d in (1, 2) for h in (3, 4)]  # 10:00, 11:00 ICT
    weather = pl.LazyFrame({
        "farm_id": ["BA_VI"] * 4,
        "observed_at": hours,
        "temperature_c": [22.0, 23.0, 30.0, 31.0],
        "relative_humidity_pct": [80.0, 82.0, 80.0, 85.0],
        "precipitation_mm": [0.5, 0.5, 0.0, 1.0],
        "thi_index": [70.0, 72.0, 82.9, 85.0],               # computed by TV3
        "is_heat_stress": [False, False, True, True],       # TV3 rule: THI > 72
    })
    prices = pl.LazyFrame({
        "product": ["WMP", "SMP", "AMF"],
        "observed_at": [datetime(2024, 12, 17, tzinfo=UTC)] * 3,
        "price": [3800.0, 2700.0, 6900.0],
    })
    indices = pl.LazyFrame({                                  # FAO wide Bronze after TV3
        "index_name": ["Dairy Price Index", "Meat Price Index"],
        "period_start": [date(2025, 1, 1)] * 2,
        "index_value": [123.3, 100.5],
    })
    ext = bm.build_fact_external(weather, prices, indices, dims["dim_farm"]).sort("date_key")
    assert ext["date_key"].to_list() == [20250101, 20250102]
    assert ext["thi_index"].to_list() == pytest.approx([72.0, 85.0])
    assert ext["is_heat_stress"].to_list() == [0, 1]           # THI = 72 exactly is not stress
    assert ext["precipitation_mm"].to_list() == pytest.approx([1.0, 1.0])
    assert ext["gdt_wmp_usd"].to_list() == pytest.approx([3800.0, 3800.0])  # carried forward
    assert ext["fao_dairy_index"].to_list() == pytest.approx([123.3, 123.3])