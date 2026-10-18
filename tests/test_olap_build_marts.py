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


def test_fact_batch_one_row_per_batch(dims):
    fs = bm.build_fact_sensor(_sensor(), dims["dim_line"], dims["dim_batch"])
    fb = bm.build_fact_batch(_mes(), fs, dims, {"B1": "BA_VI"})
    assert fb.height == 2
    b1 = fb.filter(pl.col("batch_key") == dims["dim_batch"].filter(pl.col("batch_id") == "B1")["batch_key"][0])
    assert b1["n_readings"][0] == 2
    assert b1["uht_temp_avg"][0] == pytest.approx(141.0)
    assert b1["power_kwh"][0] == pytest.approx(2.0)  # 2 s × 3600 kW / 3600
    assert b1["farm_key"].is_not_null().all()


def test_thi_matches_tv3_example():
    df = pl.DataFrame({"t": [30.0, 10.6], "rh": [80.0, 99.0]})
    thi = df.select(thi=bm.thi_expr("t", "rh"))["thi"].to_list()
    assert thi[0] == pytest.approx(82.9, abs=0.1)
    assert thi[1] == pytest.approx(51.1, abs=0.1)