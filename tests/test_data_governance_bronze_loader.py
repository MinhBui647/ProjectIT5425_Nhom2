"""Integrity and containment tests for the governance Bronze loader."""

import shutil
from pathlib import Path

import polars as pl
import pytest

from data_governance import CONTRACT_VERSION
from data_governance.bronze_loader import load_bronze
from lakehouse_storage.utils import sha256_file


FIXTURE_ROOT = Path(__file__).parent / "fixtures" / "bronze"
SENSOR_FILE = next((FIXTURE_ROOT / "sensor_telemetry").glob("*.parquet"))


def _copy_bronze_file(tmp_path: Path, *, sidecar: bool = True) -> tuple[Path, Path]:
    root = tmp_path / "bronze"
    target_dir = root / "sensor_telemetry" / "2026-10-10"
    target_dir.mkdir(parents=True)
    target = target_dir / SENSOR_FILE.name
    shutil.copy2(SENSOR_FILE, target)
    if sidecar:
        shutil.copy2(SENSOR_FILE.with_suffix(".sha256"), target.with_suffix(".sha256"))
    return root, target


def _tree_state(root: Path) -> list[tuple[str, int, int]]:
    return sorted(
        (str(path.relative_to(root)), path.stat().st_size, path.stat().st_mtime_ns)
        for path in root.rglob("*")
        if path.is_file()
    )


def test_load_valid_bronze_adds_input_lineage_without_writes(tmp_path):
    root, path = _copy_bronze_file(tmp_path)
    before = _tree_state(root)

    frame, identity = load_bronze(path, root)

    assert frame.height > 0
    assert frame.get_column("_bronze_file").unique().to_list() == [str(path.resolve())]
    assert frame.get_column("_bronze_sha256").unique().to_list() == [sha256_file(path)]
    assert frame.get_column("_contract_version").unique().to_list() == [CONTRACT_VERSION]
    assert identity.absolute_path == path.resolve()
    assert identity.table_name == "sensor_telemetry"
    assert identity.row_count == frame.height
    assert identity.sha256 == sha256_file(path)
    assert identity.source_timestamp.tzinfo is not None
    assert _tree_state(root) == before


def test_load_bronze_rejects_missing_file(tmp_path):
    root = tmp_path / "bronze"
    root.mkdir()
    missing = root / "sensor_telemetry" / "missing.parquet"

    with pytest.raises(FileNotFoundError, match="Bronze file not found"):
        load_bronze(missing, root)


def test_load_bronze_rejects_missing_sidecar(tmp_path):
    root, path = _copy_bronze_file(tmp_path, sidecar=False)

    with pytest.raises(FileNotFoundError, match="Checksum sidecar not found"):
        load_bronze(path, root)


def test_load_bronze_rejects_bad_checksum(tmp_path):
    root, path = _copy_bronze_file(tmp_path)
    path.with_suffix(".sha256").write_text("0" * 64, encoding="utf-8")

    with pytest.raises(ValueError, match="Checksum mismatch"):
        load_bronze(path, root)


def test_load_bronze_rejects_path_traversal(tmp_path):
    root = tmp_path / "bronze"
    root.mkdir()
    outside = tmp_path / "outside.parquet"
    pl.DataFrame({"value": [1]}).write_parquet(outside)

    with pytest.raises(ValueError, match="Path traversal detected"):
        load_bronze(outside, root)


def test_load_bronze_rejects_non_parquet_file(tmp_path):
    root = tmp_path / "bronze"
    source_dir = root / "sensor_telemetry"
    source_dir.mkdir(parents=True)
    path = source_dir / "sensor.csv"
    path.write_text("value\n1\n", encoding="utf-8")

    with pytest.raises(ValueError, match=".parquet extension"):
        load_bronze(path, root)


def test_load_bronze_rejects_empty_dataframe(tmp_path):
    root = tmp_path / "bronze"
    target_dir = root / "sensor_telemetry" / "2026-10-15"
    target_dir.mkdir(parents=True)
    path = target_dir / "sensor_telemetry_empty.parquet"
    pl.DataFrame(schema={"timestamp": pl.String}).write_parquet(path)
    path.with_suffix(".sha256").write_text(sha256_file(path), encoding="utf-8")

    with pytest.raises(ValueError, match="contains no rows"):
        load_bronze(path, root)

