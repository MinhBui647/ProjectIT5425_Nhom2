"""Dataset acceptance reads the artifacts actually published by the CLI."""

import hashlib
import json
import sys
from datetime import date

import polars as pl
import pytest

from lakehouse_storage.generate_mes_data import main
from scripts.verify_mes_dataset import verify_dataset
import scripts.verify_mes_dataset as verifier


@pytest.fixture
def dataset(tmp_path, monkeypatch):
    monkeypatch.setattr(sys, "argv", [
        "generator", "--date", "2025-01-01", "--days", "1", "--write-bronze",
        "--bronze-root", str(tmp_path / "bronze"), "--output-dir", str(tmp_path / "synthetic"),
    ])
    assert main() == 0
    return tmp_path


def verify(root):
    return verify_dataset(root / "bronze", root / "synthetic" / "batch_farm_map.json",
                          start=date(2025, 1, 1), days=1)


def rewrite(path, frame):
    frame.write_parquet(path)
    path.with_suffix(".sha256").write_text(hashlib.sha256(path.read_bytes()).hexdigest())


def test_acceptance_reports_file_counts_and_business_metrics(dataset):
    result = verify(dataset)
    assert result["status"] == "PASS"
    assert result["sensor_files"] == result["mes_files"] == 2
    assert result["sensor_rows"] == 57600
    assert result["mes_rows"] == result["batches"] == result["unique_quality_rows"] == 8
    assert -1 <= result["fat_uht_correlation"] <= 1


def test_checksum_mismatch_is_rejected(dataset):
    path = next((dataset / "bronze").rglob("*.sha256"))
    path.write_text("incorrect")
    with pytest.raises(ValueError, match="checksum"):
        verify(dataset)


def test_missing_sensor_file_is_rejected(dataset):
    next((dataset / "bronze" / "sensor_telemetry").rglob("*.parquet")).unlink()
    with pytest.raises(ValueError, match="files"):
        verify(dataset)


def test_duplicate_mes_batch_is_rejected_even_with_valid_checksum(dataset):
    path = next((dataset / "bronze" / "mes_lims").rglob("*.parquet"))
    frame = pl.read_parquet(path)
    batches = frame["batch_id"].to_list()
    batches[1] = batches[0]
    rewrite(path, frame.with_columns(pl.Series("batch_id", batches)))
    with pytest.raises(ValueError, match="duplicate"):
        verify(dataset)


def test_missing_farm_mapping_is_rejected(dataset):
    path = dataset / "synthetic" / "batch_farm_map.json"
    maps = json.loads(path.read_text())
    maps[0].pop(next(iter(maps[0])))
    path.write_text(json.dumps(maps))
    with pytest.raises(ValueError, match="mapping"):
        verify(dataset)


def test_duplicate_sensor_timestamp_is_rejected_with_valid_checksum(dataset):
    path = next((dataset / "bronze" / "sensor_telemetry").rglob("*.parquet"))
    frame = pl.read_parquet(path)
    times = frame["timestamp"].to_list()
    times[1] = times[0]
    rewrite(path, frame.with_columns(pl.Series("timestamp", times)))
    with pytest.raises(ValueError, match="timestamp"):
        verify(dataset)


def test_nonfinite_measure_is_rejected_with_valid_checksum(dataset):
    path = next((dataset / "bronze" / "mes_lims").rglob("*.parquet"))
    frame = pl.read_parquet(path).with_columns(pl.lit(float("nan")).alias("fat_in"))
    rewrite(path, frame)
    with pytest.raises(ValueError, match="finite"):
        verify(dataset)


@pytest.mark.parametrize("destination", ["bronze/report.json", "synthetic/batch_farm_map.json"])
def test_report_cannot_overwrite_dataset_artifacts(dataset, monkeypatch, destination):
    mapping = dataset / "synthetic" / "batch_farm_map.json"
    original = mapping.read_bytes()
    monkeypatch.setattr(sys, "argv", [
        "verify", "--bronze-root", str(dataset / "bronze"), "--mapping-file", str(mapping),
        "--days", "1", "--output", str(dataset / destination),
    ])
    with pytest.raises(SystemExit) as error:
        verifier.main()
    assert error.value.code == 2
    assert mapping.read_bytes() == original


def test_constant_fat_produces_readable_failure_report(dataset, monkeypatch):
    for path in (dataset / "bronze" / "mes_lims").rglob("*.parquet"):
        rewrite(path, pl.read_parquet(path).with_columns(pl.lit(3.5).alias("fat_in")))
    output = dataset / "acceptance.json"
    monkeypatch.setattr(sys, "argv", [
        "verify", "--bronze-root", str(dataset / "bronze"),
        "--mapping-file", str(dataset / "synthetic" / "batch_farm_map.json"),
        "--days", "1", "--output", str(output),
    ])
    assert verifier.main() == 1
    result = json.loads(output.read_text())
    assert result["status"] == "FAIL"
    assert "variance" in result["error"]
