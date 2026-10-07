"""Regression tests for repeat crawls, overlapping windows and source revisions."""
import json
from pathlib import Path
import subprocess
import sys

import polars as pl
import pytest

from crawling_ingestion import dedup
from lakehouse_storage.bronze_writer import write_bronze_batch, verify_checksum
from lakehouse_storage.schemas import get_bronze_schema


def record(table):
    # Supply the exact required raw schema, including FDA's original fields.
    row = {field.name: "value" for field in get_bronze_schema(table) if not field.nullable}
    row["_created_at"] = "2025-07-01"
    row.update({
        "weather": {"farm_id": "MOC_CHAU", "observed_at": "2025-01-01T00:00",
                    "temperature_celsius": 20.0, "relative_humidity_pct": 70.0, "precipitation_mm": 0.0},
        "market_prices": {"ProductGroupCode": "WMP", "EventNumber": "100",
                          "EventDate": "2025-01-07T00:00:00Z", "AveragePublishedPrice": "3804"},
        "food_recalls": {"recall_number": "F-0001-2025", "report_date": "20250101",
                         "status": "Ongoing", "openfda": '{"a":1,"b":2}'},
        "usda_market_prices": {"source": "USDA_NDPSR", "product": "BUTTER",
                               "contract": "WEEKLY_SURVEY", "observed_at": "2025-01-04T00:00:00Z",
                               "price": 2.5, "currency": "USD", "unit": "lb",
                               "_raw_record": '{"Butter_Price":"2.5","Butter_Sales":"100"}'},
        "market_indices": {"source": "FAO", "index_name": "DAIRY", "period_start": "2025-01-01",
                           "index_value": 143.1, "base_period": "2014-2016=100"},
    }[table])
    return row


def write(rows, table, root):
    return dedup.write_unique_bronze(pl.DataFrame(rows), table, root)


@pytest.mark.parametrize("table", list(dedup.KEYS))
def test_repeat_all_five_sources_ignores_fetch_metadata(table, tmp_path):
    row = record(table)
    first = write([row], table, tmp_path)
    changed_fetch = {**row, "_created_at": "2025-08-01", "_generated_at": "later",
                     "ingested_at": "later", "_source_url": "https://new-fetch-url",
                     "meta_last_updated": "later", "meta_results_skip": 100,
                     "meta_results_limit": 10, "meta_results_total": 999, "_expected_total": 999}
    second = write([changed_fetch], table, tmp_path)
    assert first["written_rows"] == 1 and second["unchanged_rows"] == 1
    assert second["paths"] == [] and len(list(tmp_path.rglob("*.parquet"))) == 1
    stored = pl.read_parquet(first["paths"][0])
    assert len(stored["_record_sha256"][0]) == 64
    assert verify_checksum(first["paths"][0])


def test_overlapping_windows_and_farms(tmp_path):
    row = record("weather")
    hour1 = {**row, "observed_at": "2025-01-01T01:00"}
    hour2 = {**row, "observed_at": "2025-01-01T02:00"}
    write([row, hour1], "weather", tmp_path)
    result = write([hour1, hour2, {**row, "farm_id": "BA_VI"}], "weather", tmp_path)
    assert result["written_rows"] == 2 and result["unchanged_rows"] == 1
    assert sum(len(pl.read_parquet(p)) for p in tmp_path.rglob("*.parquet")) == 4


@pytest.mark.parametrize("table,field,new_value", [
    ("weather", "temperature_celsius", 21.0),
    ("market_prices", "AveragePublishedPrice", "3900"),
    ("food_recalls", "status", "Terminated"),
    ("usda_market_prices", "price", 2.7),
    ("market_indices", "index_value", 144.0),
])
def test_revisions_and_reversion_preserve_history(table, field, new_value, tmp_path):
    row = record(table)
    first = write([row], table, tmp_path)
    original_bytes = Path(first["paths"][0]).read_bytes()
    revised = {**row, field: new_value}
    assert write([revised], table, tmp_path)["revised_rows"] == 1
    assert write([revised], table, tmp_path)["written_rows"] == 0
    assert write([row], table, tmp_path)["revised_rows"] == 1
    assert write([row], table, tmp_path)["written_rows"] == 0
    assert len(list(tmp_path.rglob("*.parquet"))) == 3
    assert Path(first["paths"][0]).read_bytes() == original_bytes


def test_legacy_file_without_fingerprint_columns(tmp_path):
    row = record("weather")
    # Legacy batches can contain multiple event days in the first day's folder.
    next_day = {**row, "observed_at": "2025-01-02T00:00"}
    old = write_bronze_batch(pl.DataFrame([row, next_day]), "weather", tmp_path)
    result = write([next_day], "weather", tmp_path)
    assert result["unchanged_rows"] == 1 and result["paths"] == []
    assert "_record_sha256" not in pl.read_parquet(old).columns


def test_duplicate_rows_within_batch(tmp_path):
    row = record("weather")
    result = write([row, row, row], "weather", tmp_path)
    assert result["written_rows"] == 1 and result["batch_duplicates"] == 2


def test_conflicting_duplicate_key_rejected_before_writing(tmp_path):
    row = record("weather")
    with pytest.raises(ValueError, match="Conflicting"):
        write([row, {**row, "temperature_celsius": 99.0}], "weather", tmp_path)
    assert not list(tmp_path.rglob("*.parquet"))


def test_missing_key_and_empty_batch(tmp_path):
    row = record("weather")
    with pytest.raises(ValueError, match="business key"):
        write([{**row, "farm_id": None}], "weather", tmp_path)
    assert dedup.write_unique_bronze(pl.DataFrame(), "weather", tmp_path)["paths"] == []


def test_null_is_not_zero_and_numeric_promotion_is_stable(tmp_path):
    row = {**record("usda_market_prices"), "price": None}
    write([row], "usda_market_prices", tmp_path)
    assert write([row], "usda_market_prices", tmp_path)["written_rows"] == 0
    assert write([{**row, "price": 0}], "usda_market_prices", tmp_path)["revised_rows"] == 1
    assert write([{**row, "price": 0.0}], "usda_market_prices", tmp_path)["written_rows"] == 0


def test_json_order_and_column_order_do_not_change_fingerprint(tmp_path):
    row = record("food_recalls")
    write([row], "food_recalls", tmp_path)
    changed = dict(reversed(list({**row, "openfda": '{"b": 2, "a": 1}'}.items())))
    assert write([changed], "food_recalls", tmp_path)["written_rows"] == 0


def test_raw_record_changes_are_preserved(tmp_path):
    row = record("usda_market_prices")
    write([row], "usda_market_prices", tmp_path)
    revised = {**row, "_raw_record": '{"Butter_Price":"2.5","Butter_Sales":"200"}'}
    assert write([revised], "usda_market_prices", tmp_path)["revised_rows"] == 1


def test_equivalent_utc_timestamps(tmp_path):
    row = record("weather")
    write([row], "weather", tmp_path)
    assert write([{**row, "observed_at": "2025-01-01T07:00:00+07:00"}], "weather", tmp_path)["written_rows"] == 0


@pytest.mark.parametrize("damage", ["checksum", "missing_checksum", "parquet"])
def test_corrupt_history_fails_closed(tmp_path, damage):
    row = record("weather")
    file = Path(write([row], "weather", tmp_path)["paths"][0])
    if damage == "checksum":
        file.with_suffix(".sha256").write_text("bad hash")
    elif damage == "missing_checksum":
        file.with_suffix(".sha256").unlink()
    else:
        file.write_bytes(b"broken parquet")
    with pytest.raises(ValueError, match="checksum"):
        write([{**row, "observed_at": "2025-01-02T00:00"}], "weather", tmp_path)
    assert len(list(tmp_path.rglob("*.parquet"))) == 1


def test_retry_after_failure_between_days(monkeypatch, tmp_path):
    row = record("weather")
    rows = [row, {**row, "observed_at": "2025-01-02T00:00"}]
    real_writer = dedup.write_bronze_batch
    calls = 0
    def flaky(*args, **kwargs):
        nonlocal calls
        calls += 1
        if calls == 2:
            raise OSError("simulated disk failure")
        return real_writer(*args, **kwargs)
    monkeypatch.setattr(dedup, "write_bronze_batch", flaky)
    with pytest.raises(OSError):
        write(rows, "weather", tmp_path)
    monkeypatch.setattr(dedup, "write_bronze_batch", real_writer)
    result = write(rows, "weather", tmp_path)
    assert result["written_rows"] == 1 and result["unchanged_rows"] == 1
    assert len(list(tmp_path.rglob("*.parquet"))) == 2


def test_cross_process_lock(tmp_path):
    src = str(Path(dedup.__file__).resolve().parents[1])
    script = (
        "import sys; sys.dont_write_bytecode=True; sys.path.insert(0," + repr(src) + "); "
        "from pathlib import Path; from crawling_ingestion.dedup import _table_lock; "
        "lock=_table_lock(Path(" + repr(str(tmp_path)) + "), 'weather'); lock.__enter__()"
    )
    with dedup._table_lock(tmp_path, "weather"):
        proc = subprocess.run([sys.executable, "-c", script], capture_output=True, text=True, timeout=30)
        assert proc.returncode != 0 and "Another crawler" in proc.stderr
    with dedup._table_lock(tmp_path, "weather"):
        pass


def test_runner_uses_dedup_by_default(monkeypatch, tmp_path):
    import importlib
    runner = importlib.import_module("crawling_ingestion.run_crawlers")
    row = record("market_indices")
    monkeypatch.setattr(runner, "get_fao_dairyindex_data", lambda *args: pl.DataFrame([row]))
    assert len(runner.run_crawlers("2025-01-01", "2025-01-31", ["fao"], tmp_path)) == 1
    row["_created_at"] = "2025-08-01"
    assert runner.run_crawlers("2025-01-01", "2025-01-31", ["fao"], tmp_path) == []
