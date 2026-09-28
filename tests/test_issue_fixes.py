"""Regression tests for reviewed issues in lakehouse_storage."""

from datetime import date, datetime, timezone

import numpy as np
import polars as pl
import pyarrow as pa
import pytest

import sys
from pathlib import Path

SRC_PATH = Path(__file__).resolve().parents[1] / "src"
if str(SRC_PATH) not in sys.path:
    sys.path.insert(0, str(SRC_PATH))


def _sensor_silver_df(n_rows: int = 10, line_id: str = "LINE_UHT_1") -> pl.DataFrame:
    timestamps = [datetime(2026, 1, 15, 10, i % 60, i % 60) for i in range(n_rows)]
    return pl.DataFrame({
        "timestamp": timestamps,
        "line_id": [line_id] * n_rows,
        "batch_id": [None] * n_rows,
        "preheat_temp": np.random.normal(75.0, 2.0, n_rows).astype(np.float32),
        "uht_temp": np.random.normal(138.5, 0.7, n_rows).astype(np.float32),
        "homo_press_stage1": np.random.normal(200.0, 5.0, n_rows).astype(np.float32),
        "homo_press_stage2": np.random.normal(30.0, 2.0, n_rows).astype(np.float32),
        "flow_rate": np.random.normal(5000.0, 50.0, n_rows).astype(np.float32),
        "conductivity": np.random.normal(4.5, 0.1, n_rows).astype(np.float32),
        "power_kw": np.random.normal(150.0, 5.0, n_rows).astype(np.float32),
        "ingestion_date": [date(2026, 1, 15)] * n_rows,
        "_source_system": ["TEST"] * n_rows,
        "_bronze_file": ["/test/path.parquet"] * n_rows,
        "_bronze_sha256": ["abc123"] * n_rows,
        "_ingested_at": [datetime.now(timezone.utc)] * n_rows,
    })


def _bronze_sensor_df(n_rows: int = 10) -> pl.DataFrame:
    timestamps = [datetime(2026, 1, 15, 10, 0, 0).isoformat()] * n_rows
    return pl.DataFrame({
        "timestamp": timestamps,
        "line_id": ["LINE_UHT_1"] * n_rows,
        "batch_id": [None] * n_rows,
        "preheat_temp": np.random.normal(75.0, 2.0, n_rows).tolist(),
        "uht_temp": np.random.normal(138.5, 0.7, n_rows).tolist(),
        "homo_press_stage1": np.random.normal(200.0, 5.0, n_rows).tolist(),
        "homo_press_stage2": np.random.normal(30.0, 2.0, n_rows).tolist(),
        "flow_rate": np.random.normal(5000.0, 50.0, n_rows).tolist(),
        "conductivity": np.random.normal(4.5, 0.1, n_rows).tolist(),
        "power_kw": np.random.normal(150.0, 5.0, n_rows).tolist(),
        "_source_system": ["TEST"] * n_rows,
        "_generated_at": [datetime.now(timezone.utc).isoformat()] * n_rows,
    })


def _mes_bronze_df(batch_id: str, line_id: str = "LINE_UHT_1") -> pl.DataFrame:
    start = datetime(2026, 1, 15, 1, 0, 0, tzinfo=timezone.utc)
    return pl.DataFrame([{
        "batch_id": batch_id,
        "line_id": line_id,
        "production_start": start.isoformat(),
        "production_end": (start.replace(hour=3)).isoformat(),
        "sku_id": "UHT_PURE",
        "fat_in": 3.5,
        "protein_in": 3.2,
        "acidity_sh": 6.5,
        "yield_recovery_pct": 95.0,
        "microbiology_status": "PASSED",
        "batch_defect_flag": 0,
        "_source_system": "TEST",
        "_generated_at": datetime.now(timezone.utc).isoformat(),
    }])


# ISSUE-01 -----------------------------------------------------------------

class TestCreateDeltaTable:
    def test_create_with_partition_column_not_in_schema(self, tmp_path):
        from lakehouse_storage.schemas import SENSOR_TELEMETRY_SCHEMA
        from lakehouse_storage.silver_manager import (
            append_to_delta,
            create_delta_table,
            get_table_version,
            read_silver_table,
        )

        uri = str(tmp_path / "table")
        create_delta_table(
            uri,
            schema=SENSOR_TELEMETRY_SCHEMA,
            partition_by=["line_id", "ingestion_date"],
        )
        assert get_table_version(uri) == 0

        append_to_delta(
            _sensor_silver_df(5),
            uri,
            partition_by=["line_id", "ingestion_date"],
            table_name="sensor_telemetry",
        )
        assert len(read_silver_table(uri)) == 5


# ISSUE-02 -----------------------------------------------------------------

class TestStatsWithoutFullLoad:
    def test_row_count(self, tmp_path):
        from lakehouse_storage.silver_manager import (
            append_to_delta,
            get_table_stats,
        )

        uri = str(tmp_path / "table")
        append_to_delta(
            _sensor_silver_df(42),
            uri,
            partition_by=["line_id", "ingestion_date"],
            table_name="sensor_telemetry",
        )
        stats = get_table_stats(uri)
        assert stats["row_count"] == 42


# ISSUE-03 -----------------------------------------------------------------

class TestTraceBatchHistory:
    def test_history_returns_versions(self, tmp_path):
        from lakehouse_storage.silver_manager import append_to_delta
        from lakehouse_storage.time_travel import trace_batch_history

        uri = str(tmp_path / "table")
        append_to_delta(
            _mes_bronze_df("BATCH_A"),
            uri,
            partition_by=["line_id", "ingestion_date"],
            table_name="mes_lims",
        )
        append_to_delta(
            _mes_bronze_df("BATCH_B"),
            uri,
            partition_by=["line_id", "ingestion_date"],
            table_name="mes_lims",
        )

        result = trace_batch_history(uri, "BATCH_A")
        assert len(result) >= 1
        assert "_version" in result.columns
        assert "BATCH_A" in result["batch_id"].to_list()


# ISSUE-04 -----------------------------------------------------------------

class TestChecksumEnforcement:
    def test_checksum_mismatch_raises(self, tmp_path):
        from lakehouse_storage.bronze_writer import write_bronze_batch
        from lakehouse_storage.silver_manager import ingest_bronze_file

        bronze_root = tmp_path / "bronze"
        bronze_path = write_bronze_batch(
            _bronze_sensor_df(5), "sensor_telemetry", bronze_root=bronze_root
        )
        Path(bronze_path).with_suffix(".sha256").write_text("deadbeef")

        with pytest.raises(ValueError, match="Checksum verification failed"):
            ingest_bronze_file(
                bronze_path,
                str(tmp_path / "table"),
                partition_by=["line_id", "ingestion_date"],
                table_name="sensor_telemetry",
                bronze_root=bronze_root,
            )

    def test_checksum_can_be_disabled(self, tmp_path):
        from lakehouse_storage.bronze_writer import write_bronze_batch
        from lakehouse_storage.silver_manager import ingest_bronze_file

        bronze_root = tmp_path / "bronze"
        bronze_path = write_bronze_batch(
            _bronze_sensor_df(5), "sensor_telemetry", bronze_root=bronze_root
        )
        Path(bronze_path).with_suffix(".sha256").write_text("deadbeef")

        result = ingest_bronze_file(
            bronze_path,
            str(tmp_path / "table"),
            partition_by=["line_id", "ingestion_date"],
            table_name="sensor_telemetry",
            bronze_root=bronze_root,
            enforce_checksum=False,
        )
        assert result["rows_ingested"] == 5


# ISSUE-05 -----------------------------------------------------------------

class TestAliasNormalization:
    def test_s2_bronze_partition_uses_production_start(self, tmp_path):
        from lakehouse_storage.bronze_writer import write_bronze_batch

        bronze_root = tmp_path / "bronze"
        path = write_bronze_batch(_mes_bronze_df("B1"), "S2", bronze_root=bronze_root)
        assert "2026-01-15" in path
        assert "mes_lims" in path

    def test_s2_silver_casts_floats(self, tmp_path):
        from lakehouse_storage.silver_manager import append_to_delta, read_silver_table

        uri = str(tmp_path / "table")
        append_to_delta(
            _mes_bronze_df("B1"),
            uri,
            partition_by=["line_id", "ingestion_date"],
            table_name="S2",
        )
        df = read_silver_table(uri)
        assert df["fat_in"].dtype == pl.Float32
        assert df["production_start"].dtype == pl.Datetime

    def test_normalize_lowercase_alias(self):
        from lakehouse_storage.schemas import normalize_table_name

        assert normalize_table_name("s3") == "market_prices"
        assert normalize_table_name("MES_LIMS") == "mes_lims"


# ISSUE-06 -----------------------------------------------------------------

class TestExtractDateSafety:
    def test_empty_dataframe_raises(self, tmp_path):
        from lakehouse_storage.bronze_writer import write_bronze_batch

        empty = _bronze_sensor_df(0)
        with pytest.raises(ValueError, match="empty batch"):
            write_bronze_batch(empty, "sensor_telemetry", bronze_root=tmp_path / "b")

    def test_first_null_uses_later_valid_date(self, tmp_path):
        from lakehouse_storage.bronze_writer import write_bronze_batch

        df = _bronze_sensor_df(3)
        df = df.with_columns(
            pl.when(pl.int_range(pl.len()) == 0)
            .then(None)
            .otherwise(pl.col("timestamp"))
            .alias("timestamp")
        )
        path = write_bronze_batch(df, "sensor_telemetry", bronze_root=tmp_path / "b")
        assert "2026-01-15" in path


# ISSUE-07 -----------------------------------------------------------------

class TestFileSizesFromMetadata:
    def test_get_file_sizes_nonzero(self, tmp_path):
        from lakehouse_storage.maintenance import get_file_sizes
        from lakehouse_storage.silver_manager import append_to_delta

        uri = str(tmp_path / "table")
        append_to_delta(
            _sensor_silver_df(100),
            uri,
            partition_by=["line_id", "ingestion_date"],
            table_name="sensor_telemetry",
        )
        sizes = get_file_sizes(uri)
        assert sizes
        assert sizes[0]["size_bytes"] > 0


# ISSUE-09 -----------------------------------------------------------------

class TestGeneratorsUseBronze:
    def test_write_sensor_batch_adds_lineage(self, tmp_path):
        from lakehouse_storage.mock_sensor_stream import (
            generate_sensor_batch,
            write_sensor_batch,
        )

        uri = str(tmp_path / "sensor_table")
        df = generate_sensor_batch(n_samples=5, batch_id="BATCH_X", seed=1)
        write_sensor_batch(df, table_uri=uri, bronze_root=tmp_path / "bronze")

        silver = pl.scan_delta(uri).collect()
        assert "_bronze_file" in silver.columns
        assert "_bronze_sha256" in silver.columns

    def test_write_mes_batch_adds_lineage(self, tmp_path):
        from lakehouse_storage.generate_mes_data import (
            generate_mes_batch,
            write_mes_batch,
        )

        uri = str(tmp_path / "mes_table")
        df = generate_mes_batch(date=datetime(2026, 1, 15), batch_id="BATCH_M")
        write_mes_batch(df, table_uri=uri, bronze_root=tmp_path / "bronze")

        silver = pl.scan_delta(uri).collect()
        assert "_bronze_file" in silver.columns
        assert "_bronze_sha256" in silver.columns


# ISSUE-10 -----------------------------------------------------------------

class TestReadSilverWithoutLineId:
    def test_line_id_filter_ignored_for_weather(self, tmp_path):
        from lakehouse_storage.silver_manager import append_to_delta, read_silver_table

        df = pl.DataFrame({
            "farm_id": ["MOC_CHAU"] * 5,
            "observed_at": ["2026-01-15T10:00:00"] * 5,
            "temperature_c": [28.0] * 5,
            "relative_humidity_pct": [75.0] * 5,
            "precipitation_mm": [0.0] * 5,
            "ingested_at": ["2026-01-15T10:00:00+00:00"] * 5,
        })
        uri = str(tmp_path / "weather")
        append_to_delta(
            df,
            uri,
            partition_by=["farm_id", "ingestion_date"],
            table_name="weather",
        )

        result = read_silver_table(uri, line_id="LINE_UHT_1")
        assert len(result) == 5


# ISSUE-11 -----------------------------------------------------------------

def test_partition_columns_includes_bronze_metadata():
    from lakehouse_storage.schemas import PARTITION_COLUMNS

    assert PARTITION_COLUMNS["bronze_metadata"] == ["fetched_at"]


# ISSUE-14 -----------------------------------------------------------------

def test_ingest_rejects_path_traversal(tmp_path):
    from lakehouse_storage.bronze_writer import write_bronze_batch
    from lakehouse_storage.silver_manager import ingest_bronze_file

    bronze_root = tmp_path / "bronze"
    bronze_path = write_bronze_batch(
        _bronze_sensor_df(2), "sensor_telemetry", bronze_root=bronze_root
    )
    other_root = tmp_path / "other"

    with pytest.raises(ValueError, match="Path traversal"):
        ingest_bronze_file(
            bronze_path,
            str(tmp_path / "table"),
            partition_by=["line_id", "ingestion_date"],
            table_name="sensor_telemetry",
            bronze_root=other_root,
        )


# Coverage gaps ------------------------------------------------------------

def test_ingest_multiple_files(tmp_path):
    from lakehouse_storage.bronze_writer import ingest_multiple_files, write_bronze_batch

    bronze_root = tmp_path / "bronze"
    p1 = write_bronze_batch(_bronze_sensor_df(2), "sensor_telemetry", bronze_root=bronze_root)
    p2 = write_bronze_batch(_bronze_sensor_df(3), "sensor_telemetry", bronze_root=bronze_root)

    results = ingest_multiple_files("sensor_telemetry", [p1, p2], bronze_root=bronze_root)
    assert len(results) == 2
    assert all(r["verified"] for r in results)


def test_vacuum_all_tables(tmp_path):
    from lakehouse_storage.maintenance import vacuum_all_tables
    from lakehouse_storage.silver_manager import append_to_delta

    uri = str(tmp_path / "table")
    append_to_delta(
        _sensor_silver_df(10),
        uri,
        partition_by=["line_id", "ingestion_date"],
        table_name="sensor_telemetry",
    )
    results = vacuum_all_tables([uri], retention_hours=0, dry_run=True)
    assert len(results) == 1
    assert results[0]["table_uri"] == uri
    assert "files_removed" in results[0]


# Conftest fixtures (C1/C2) -------------------------------------------------

def test_sample_mes_batch_fixture_builds(sample_mes_batch):
    assert len(sample_mes_batch) == 10


def test_sample_market_prices_batch_fixture_builds(sample_market_prices_batch):
    assert {len(v) for v in sample_market_prices_batch.values()} == {50}


# ISSUE (I5) per-file error handling ---------------------------------------

def test_ingest_multiple_files_reports_traversal(tmp_path):
    from lakehouse_storage.bronze_writer import ingest_multiple_files, write_bronze_batch

    bronze_root = tmp_path / "bronze"
    good = write_bronze_batch(
        _bronze_sensor_df(2), "sensor_telemetry", bronze_root=bronze_root
    )
    outside = tmp_path / "outside.parquet"
    outside.write_bytes(Path(good).read_bytes())

    results = ingest_multiple_files(
        "sensor_telemetry", [good, str(outside)], bronze_root=bronze_root
    )
    assert results[0]["verified"] is True
    assert "error" in results[1]


# ISSUE (I4) strict cast mode ----------------------------------------------

def test_strict_cast_raises_for_naive_timestamp(tmp_path):
    from lakehouse_storage.silver_manager import append_to_delta

    df = pl.DataFrame({
        "farm_id": ["MOC_CHAU"],
        "observed_at": ["2026-01-15T10:00:00"],
        "temperature_c": [28.0],
        "relative_humidity_pct": [75.0],
        "precipitation_mm": [0.0],
        "ingested_at": ["2026-01-15T10:00:00"],
    })
    with pytest.raises(ValueError, match="Failed to cast non-nullable column"):
        append_to_delta(
            df,
            str(tmp_path / "weather"),
            partition_by=["farm_id", "ingestion_date"],
            table_name="weather",
        )


def test_cast_to_schema_non_strict_keeps_original():
    from lakehouse_storage.schemas import WEATHER_SCHEMA
    from lakehouse_storage.silver_manager import _cast_to_schema

    table = pa.table({"ingested_at": pa.array(["2026-01-15T10:00:00"])})
    out = _cast_to_schema(table, WEATHER_SCHEMA, strict=False)
    assert out.column("ingested_at").type == pa.string()


# ISSUE (I8) safe vacuum default -------------------------------------------

def test_run_full_maintenance_vacuum_defaults_to_dry_run(tmp_path):
    from lakehouse_storage.maintenance import run_full_maintenance
    from lakehouse_storage.silver_manager import append_to_delta

    uri = str(tmp_path / "table")
    append_to_delta(
        _sensor_silver_df(10),
        uri,
        partition_by=["line_id", "ingestion_date"],
        table_name="sensor_telemetry",
    )
    result = run_full_maintenance(uri, compact=False, zorder_cols=None)
    assert result["vacuum"]["dry_run"] is True
