"""Integrity checks for the immutable Bronze fixture set."""

from __future__ import annotations

import hashlib
import json
from pathlib import Path

import polars as pl
import pyarrow as pa

from lakehouse_storage.schemas import get_bronze_schema


FIXTURE_ROOT = Path(__file__).parent / "fixtures" / "bronze"
MANIFEST_PATH = FIXTURE_ROOT / "manifest.json"
EXPECTED_TABLES = {
    "sensor_telemetry",
    "mes_lims",
    "weather",
    "market_prices",
    "market_indices",
    "food_recalls",
}


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for chunk in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _types_compatible(actual: pa.DataType, expected: pa.DataType) -> bool:
    """Treat Arrow string and large_string as one logical Bronze type."""
    if pa.types.is_string(actual) or pa.types.is_large_string(actual):
        return pa.types.is_string(expected) or pa.types.is_large_string(expected)
    return actual == expected


def test_bronze_fixture_manifest_is_complete_and_reproducible():
    manifest = json.loads(MANIFEST_PATH.read_text(encoding="utf-8"))

    assert manifest["manifest_version"] == 1
    assert set(manifest["tables"]) == EXPECTED_TABLES

    for table_name, metadata in manifest["tables"].items():
        table_dir = FIXTURE_ROOT / table_name
        parquet_path = table_dir / metadata["file_name"]
        sidecar_path = table_dir / metadata["sidecar_name"]

        assert parquet_path.is_file()
        assert sidecar_path.is_file()
        assert parquet_path.stat().st_size == metadata["size_bytes"]

        actual_sha256 = _sha256(parquet_path)
        assert actual_sha256 == metadata["sha256"]
        assert sidecar_path.read_text(encoding="utf-8").strip() == actual_sha256

        frame = pl.read_parquet(parquet_path)
        assert frame.height == metadata["row_count"]
        assert frame.columns == metadata["columns"]
        assert {name: str(dtype) for name, dtype in frame.schema.items()} == metadata["dtypes"]

        expected_schema = get_bronze_schema(table_name)
        actual_schema = frame.to_arrow().schema
        assert set(actual_schema.names).issubset(expected_schema.names)
        for field in actual_schema:
            assert _types_compatible(
                field.type,
                expected_schema.field(field.name).type,
            )


def test_usda_is_not_part_of_the_mvp_fixture_set():
    assert not (FIXTURE_ROOT / "market_reports").exists()
    assert not (FIXTURE_ROOT / "usda_market_prices").exists()
