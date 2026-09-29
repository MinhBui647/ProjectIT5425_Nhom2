"""Tests for silver_manager module.

Note: These tests use local filesystem for Delta tables instead of MinIO.
For full MinIO tests, run with Docker MinIO container.
"""

from datetime import date, datetime, timezone
from pathlib import Path

import numpy as np
import polars as pl
import pytest

from lakehouse_storage.schemas import SENSOR_TELEMETRY_SCHEMA, PARTITION_COLUMNS


class TestSilverManagerLocal:
    """Test silver_manager functions with local filesystem.

    These tests use local Delta tables for speed and isolation.
    """

    @pytest.fixture
    def local_table_uri(self, tmp_path):
        """Create a local Delta table URI."""
        table_path = tmp_path / "silver" / "test_table"
        return str(table_path)

    def test_append_to_empty_table(self, local_table_uri):
        """Can append to empty Delta table."""
        from lakehouse_storage.silver_manager import append_to_delta, get_table_version

        df = self._create_sensor_df(100)

        # First append should work
        append_to_delta(
            df,
            local_table_uri,
            partition_by=["line_id", "ingestion_date"],
            table_name="sensor_telemetry",
        )

        version = get_table_version(local_table_uri)
        assert version == 0

    def test_append_increments_version(self, local_table_uri):
        """Each append increments Delta version."""
        from lakehouse_storage.silver_manager import append_to_delta, get_table_version

        partition_cols = ["line_id", "ingestion_date"]

        # Append first batch
        df1 = self._create_sensor_df(100)
        append_to_delta(df1, local_table_uri, partition_by=partition_cols, table_name="sensor_telemetry")

        # Append second batch
        df2 = self._create_sensor_df(50)
        append_to_delta(df2, local_table_uri, partition_by=partition_cols, table_name="sensor_telemetry")

        version = get_table_version(local_table_uri)
        assert version == 1

    def test_append_preserves_data(self, local_table_uri):
        """Appending doesn't delete existing data."""
        from lakehouse_storage.silver_manager import append_to_delta, read_silver_table

        partition_cols = ["line_id", "ingestion_date"]

        # Append first batch
        df1 = self._create_sensor_df(100)
        append_to_delta(df1, local_table_uri, partition_by=partition_cols, table_name="sensor_telemetry")

        # Append second batch
        df2 = self._create_sensor_df(50)
        append_to_delta(df2, local_table_uri, partition_by=partition_cols, table_name="sensor_telemetry")

        # Read all data
        result = read_silver_table(local_table_uri)

        assert len(result) == 150  # 100 + 50

    def test_read_with_line_id_filter(self, local_table_uri):
        """Can filter by line_id."""
        from lakehouse_storage.silver_manager import append_to_delta, read_silver_table

        partition_cols = ["line_id", "ingestion_date"]

        # Append LINE_UHT_1 data
        df1 = self._create_sensor_df(100, line_id="LINE_UHT_1")
        append_to_delta(df1, local_table_uri, partition_by=partition_cols, table_name="sensor_telemetry")

        # Append LINE_UHT_2 data
        df2 = self._create_sensor_df(50, line_id="LINE_UHT_2")
        append_to_delta(df2, local_table_uri, partition_by=partition_cols, table_name="sensor_telemetry")

        # Filter by LINE_UHT_1
        result = read_silver_table(local_table_uri, line_id="LINE_UHT_1")

        assert len(result) == 100
        assert result["line_id"].unique().to_list() == ["LINE_UHT_1"]

    def test_read_with_date_filter(self, local_table_uri):
        """Can filter by date range."""
        from lakehouse_storage.silver_manager import append_to_delta, read_silver_table

        partition_cols = ["line_id", "ingestion_date"]

        df = self._create_sensor_df(100, date_str="2026-01-15")
        append_to_delta(df, local_table_uri, partition_by=partition_cols, table_name="sensor_telemetry")

        # Read with date filter
        result = read_silver_table(
            local_table_uri,
            date_from="2026-01-14",
            date_to="2026-01-16",
        )

        assert len(result) == 100

    def test_export_silver_parquet(self, local_table_uri, tmp_path):
        """Can export Silver table to Parquet."""
        from lakehouse_storage.silver_manager import append_to_delta, export_silver_parquet

        partition_cols = ["line_id", "ingestion_date"]

        df = self._create_sensor_df(500)
        append_to_delta(df, local_table_uri, partition_by=partition_cols, table_name="sensor_telemetry")

        export_path = tmp_path / "export" / "data.parquet"
        exported = export_silver_parquet(local_table_uri, export_path)

        assert Path(exported).exists()

        # Verify data
        exported_df = pl.read_parquet(exported)
        assert len(exported_df) == 500

    def test_get_table_stats(self, local_table_uri):
        """Can get table statistics."""
        from lakehouse_storage.silver_manager import append_to_delta, get_table_stats

        partition_cols = ["line_id", "ingestion_date"]

        df = self._create_sensor_df(200)
        append_to_delta(df, local_table_uri, partition_by=partition_cols, table_name="sensor_telemetry")

        stats = get_table_stats(local_table_uri)

        assert "version" in stats
        assert "row_count" in stats
        assert "file_count" in stats

    # Helper methods
    def _create_sensor_df(
        self,
        n_rows: int,
        line_id: str = "LINE_UHT_1",
        date_str: str = "2026-01-15",
    ) -> pl.DataFrame:
        """Create sensor_telemetry DataFrame with all required columns."""
        timestamps = [
            datetime(2026, 1, 15, 10, i % 60, i % 60)
            for i in range(n_rows)
        ]

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
            "_bronze_sha256": ["abc123def456"] * n_rows,
            "_ingested_at": [datetime.now(timezone.utc).replace(tzinfo=None)] * n_rows,
        })


class TestIngestBronzeToSilver:
    """Test Bronze to Silver ingestion."""

    @pytest.fixture
    def bronze_and_silver(self, tmp_path):
        """Setup Bronze and Silver paths."""
        bronze_path = tmp_path / "bronze"
        silver_path = tmp_path / "silver"
        bronze_path.mkdir(parents=True)
        silver_path.mkdir(parents=True)
        return str(bronze_path), str(silver_path)

    def test_ingest_adds_lineage_columns(self, bronze_and_silver, tmp_path):
        """ingest_bronze_file adds lineage columns."""
        from lakehouse_storage.bronze_writer import write_bronze_batch
        from lakehouse_storage.silver_manager import ingest_bronze_file

        bronze_root, silver_root = bronze_and_silver
        silver_uri = str(tmp_path / "silver" / "table")

        # Write to Bronze
        df = self._create_bronze_sensor_df(50)
        bronze_path = write_bronze_batch(df, "sensor_telemetry", bronze_root=bronze_root)

        # Ingest to Silver
        result = ingest_bronze_file(
            bronze_path,
            silver_uri,
            partition_by=["line_id", "ingestion_date"],
            table_name="sensor_telemetry",
            bronze_root=bronze_root,
        )

        assert result["rows_ingested"] == 50
        assert "sha256" in result

    def test_ingest_calculates_sha256(self, bronze_and_silver, tmp_path):
        """ingest_bronze_file calculates SHA-256 of Bronze file."""
        from lakehouse_storage.bronze_writer import write_bronze_batch
        from lakehouse_storage.silver_manager import ingest_bronze_file

        bronze_root, silver_root = bronze_and_silver
        silver_uri = str(tmp_path / "silver" / "table")

        # Write to Bronze
        df = self._create_bronze_sensor_df(100)
        bronze_path = write_bronze_batch(df, "sensor_telemetry", bronze_root=bronze_root)

        # Ingest
        result = ingest_bronze_file(
            bronze_path,
            silver_uri,
            partition_by=["line_id", "ingestion_date"],
            table_name="sensor_telemetry",
        )

        # SHA-256 should be 64 characters (hex)
        assert len(result["sha256"]) == 64

    def test_ingest_nonexistent_file_raises(self, bronze_and_silver, tmp_path):
        """Raises error for nonexistent Bronze file."""
        from lakehouse_storage.silver_manager import ingest_bronze_file

        _, silver_root = bronze_and_silver
        silver_uri = str(tmp_path / "silver" / "table")

        with pytest.raises(FileNotFoundError):
            ingest_bronze_file(
                "/nonexistent/file.parquet",
                silver_uri,
                partition_by=["line_id", "ingestion_date"],
            )

    # Helper methods
    def _create_bronze_sensor_df(self, n_rows: int) -> pl.DataFrame:
        """Create Bronze sensor_telemetry DataFrame."""
        base_time = datetime(2026, 1, 15, 10, 0, 0)
        timestamps = [base_time.isoformat() for _ in range(n_rows)]

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


class TestDeleteFromDelta:
    """Test delete operations."""

    def test_delete_removes_rows(self, tmp_path):
        """Delete removes matching rows."""
        from lakehouse_storage.silver_manager import append_to_delta, delete_from_delta, read_silver_table

        table_uri = str(tmp_path / "table")

        # Append data
        df = self._create_sensor_df(100, line_id="LINE_UHT_1")
        append_to_delta(df, table_uri, partition_by=["line_id", "ingestion_date"], table_name="sensor_telemetry")

        df2 = self._create_sensor_df(50, line_id="LINE_UHT_2")
        append_to_delta(df2, table_uri, partition_by=["line_id", "ingestion_date"], table_name="sensor_telemetry")

        # Delete LINE_UHT_1 rows
        result = delete_from_delta(table_uri, "line_id = 'LINE_UHT_1'")

        assert result["rows_deleted"] == 100

        # Verify remaining
        remaining = read_silver_table(table_uri)
        assert len(remaining) == 50
        assert remaining["line_id"].unique().to_list() == ["LINE_UHT_2"]

    # Helper methods
    def _create_sensor_df(self, n_rows: int, line_id: str = "LINE_UHT_1") -> pl.DataFrame:
        """Create sensor_telemetry DataFrame with all required columns."""
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
            "_bronze_sha256": ["abc123def456"] * n_rows,
            "_ingested_at": [datetime.now(timezone.utc).replace(tzinfo=None)] * n_rows,
        })


class TestLegacyS1Alias:
    """Test backward compatibility with S1 alias."""

    def test_s1_alias_works_for_append(self, tmp_path):
        """S1 alias works for append_to_delta."""
        from lakehouse_storage.silver_manager import append_to_delta, get_table_version

        table_uri = str(tmp_path / "table")
        df = self._create_sensor_df(50)

        # Use S1 alias
        append_to_delta(
            df,
            table_uri,
            partition_by=["line_id", "ingestion_date"],
            table_name="S1",  # Legacy alias
        )

        version = get_table_version(table_uri)
        assert version == 0

    # Helper
    def _create_sensor_df(self, n_rows: int) -> pl.DataFrame:
        """Create sensor_telemetry DataFrame."""
        timestamps = [datetime(2026, 1, 15, 10, i % 60, i % 60) for i in range(n_rows)]
        return pl.DataFrame({
            "timestamp": timestamps,
            "line_id": ["LINE_UHT_1"] * n_rows,
            "batch_id": [None] * n_rows,
            "preheat_temp": np.random.normal(75.0, 2.0, n_rows).astype(np.float32),
            "uht_temp": np.random.normal(138.5, 0.7, n_rows).astype(np.float32),
            "homo_press_stage1": np.random.normal(200.0, 5.0, n_rows).astype(np.float32),
            "homo_press_stage2": np.random.normal(30.0, 2.0, n_rows).astype(np.float32),
            "flow_rate": np.random.normal(5000.0, 50.0, n_rows).astype(np.float32),
            "conductivity": np.random.normal(4.5, 0.1, n_rows).astype(np.float32),
            "power_kw": np.random.normal(150.0, 5.0, n_rows).astype(np.float32),
        })


class TestWriteSilver:
    """Test the high-level write_silver entry point."""

    @pytest.fixture
    def bronze_root(self, tmp_path):
        """Create an isolated Bronze vault root."""
        root = tmp_path / "bronze"
        root.mkdir(parents=True)
        return str(root)

    def test_write_silver_adds_lineage_and_version(self, bronze_root, tmp_path):
        """Happy path: version increments and lineage columns are set."""
        from lakehouse_storage.bronze_writer import write_bronze_batch
        from lakehouse_storage.silver_manager import (
            get_table_version,
            read_silver_table,
            write_silver,
        )

        df = self._create_bronze_sensor_df(10)
        bronze_path = write_bronze_batch(
            df, "sensor_telemetry", bronze_root=bronze_root
        )
        table_uri = str(tmp_path / "silver" / "sensor_telemetry")

        version = write_silver(
            df,
            "sensor_telemetry",
            bronze_path,
            table_uri=table_uri,
            bronze_root=bronze_root,
        )

        assert version == 0
        assert get_table_version(table_uri) == 0

        result = read_silver_table(table_uri)
        assert len(result) == 10

        expected_file = str(Path(bronze_path).resolve())
        expected_sha = Path(bronze_path).with_suffix(".sha256").read_text().strip()
        assert result["_bronze_file"].unique().to_list() == [expected_file]
        assert result["_bronze_sha256"].unique().to_list() == [expected_sha]

    def test_write_silver_empty_df_returns_minus_one(self, bronze_root, tmp_path):
        """Empty DataFrame short-circuits without creating a Delta table."""
        from lakehouse_storage.silver_manager import write_silver

        table_uri = str(tmp_path / "silver" / "sensor_telemetry")
        missing = Path(bronze_root) / "sensor_stream" / "2026-01-15" / "x.parquet"

        version = write_silver(
            pl.DataFrame(),
            "sensor_telemetry",
            missing,
            table_uri=table_uri,
            bronze_root=bronze_root,
        )

        assert version == -1
        assert not Path(table_uri).exists()

    def test_write_silver_missing_bronze_raises(self, bronze_root, tmp_path):
        """Raises FileNotFoundError for a missing Bronze file inside the root."""
        from lakehouse_storage.silver_manager import write_silver

        df = self._create_bronze_sensor_df(5)
        missing = Path(bronze_root) / "sensor_stream" / "2026-01-15" / "missing.parquet"

        with pytest.raises(FileNotFoundError):
            write_silver(
                df,
                "sensor_telemetry",
                missing,
                table_uri=str(tmp_path / "silver" / "sensor_telemetry"),
                bronze_root=bronze_root,
            )

    def test_write_silver_checksum_mismatch_raises(self, bronze_root, tmp_path):
        """Raises ValueError when the Bronze file no longer matches its sidecar."""
        from lakehouse_storage.bronze_writer import write_bronze_batch
        from lakehouse_storage.silver_manager import write_silver

        df = self._create_bronze_sensor_df(5)
        bronze_path = write_bronze_batch(
            df, "sensor_telemetry", bronze_root=bronze_root
        )
        with Path(bronze_path).open("ab") as f:
            f.write(b"tampered")

        with pytest.raises(ValueError):
            write_silver(
                df,
                "sensor_telemetry",
                bronze_path,
                table_uri=str(tmp_path / "silver" / "sensor_telemetry"),
                bronze_root=bronze_root,
            )

    def test_write_silver_path_traversal_raises(self, bronze_root, tmp_path):
        """Raises ValueError when source_bronze escapes bronze_root."""
        from lakehouse_storage.silver_manager import write_silver

        df = self._create_bronze_sensor_df(5)
        outside = tmp_path / "outside.parquet"

        with pytest.raises(ValueError):
            write_silver(
                df,
                "sensor_telemetry",
                outside,
                table_uri=str(tmp_path / "silver" / "sensor_telemetry"),
                bronze_root=bronze_root,
            )

    # Helper methods
    def _create_bronze_sensor_df(self, n_rows: int) -> pl.DataFrame:
        """Create a raw Bronze sensor_telemetry DataFrame."""
        base_time = datetime(2026, 1, 15, 10, 0, 0)
        timestamps = [base_time.isoformat() for _ in range(n_rows)]

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


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
