"""Tests for the one-command Sensor -> Bronze -> Silver demo."""

from datetime import datetime, timezone

import pytest

from lakehouse_storage.run_sensor_demo import (
    build_demo_silver_uri,
    parse_utc_datetime,
    run_sensor_demo,
)


def test_run_sensor_demo_local(tmp_path):
    bronze_root = tmp_path / "bronze"
    silver_uri = str(tmp_path / "silver" / "sensor_demo")

    result = run_sensor_demo(
        run_id="unit-test-001",
        samples=5,
        line_id="LINE_TEST",
        batch_id="BATCH_TEST",
        seed=42,
        start_time=datetime(2026, 10, 10, 9, 0, tzinfo=timezone.utc),
        bronze_root=bronze_root,
        silver_table_uri=silver_uri,
    )

    assert result["status"] == "PASS"
    assert result["rows_ingested"] == 5
    assert result["silver_rows"] == 5
    assert result["silver_columns"] == 16
    assert result["delta_version"] == 0
    assert result["delta_data_files"] == 1

    bronze_files = list(bronze_root.rglob("*.parquet"))
    assert len(bronze_files) == 1
    assert bronze_files[0].with_suffix(".sha256").exists()


def test_run_sensor_demo_rejects_existing_delta_target(tmp_path):
    bronze_root = tmp_path / "bronze"
    silver_uri = str(tmp_path / "silver" / "sensor_demo")
    kwargs = {
        "run_id": "unit-test-repeat",
        "samples": 3,
        "line_id": "LINE_TEST",
        "batch_id": "BATCH_TEST",
        "seed": 42,
        "start_time": datetime(2026, 10, 10, 9, 0, tzinfo=timezone.utc),
        "bronze_root": bronze_root,
        "silver_table_uri": silver_uri,
    }

    run_sensor_demo(**kwargs)
    bronze_count = len(list(bronze_root.rglob("*.parquet")))

    with pytest.raises(FileExistsError, match="already exists"):
        run_sensor_demo(**kwargs)

    assert len(list(bronze_root.rglob("*.parquet"))) == bronze_count


def test_demo_argument_validation():
    assert parse_utc_datetime("2026-10-10T09:00:00+07:00") == datetime(
        2026, 10, 10, 2, 0, tzinfo=timezone.utc
    )
    assert build_demo_silver_uri("demo-001").endswith(
        "/02_silver_curated/_demo/sensor_telemetry/demo-001"
    )

    with pytest.raises(ValueError, match="timezone"):
        parse_utc_datetime("2026-10-10T09:00:00")
    with pytest.raises(ValueError, match="run_id"):
        build_demo_silver_uri("../unsafe")
