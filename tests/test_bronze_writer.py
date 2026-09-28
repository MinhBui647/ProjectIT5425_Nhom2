"""Tests for bronze_writer module."""

import hashlib
from datetime import datetime, timezone
from pathlib import Path

import numpy as np
import polars as pl
import pytest

from lakehouse_storage.bronze_writer import (
    write_bronze_batch,
    verify_checksum,
    list_bronze_files,
    read_bronze_file,
)


class TestWriteBronzeBatch:
    """Test write_bronze_batch function."""

    def test_write_creates_parquet_and_sidecar(self, tmp_path):
        """Write creates .parquet and .sha256 files."""
        df = self._create_valid_sensor_df(10)

        bronze_root = tmp_path / "bronze"
        path = write_bronze_batch(df, "sensor_telemetry", bronze_root=bronze_root)

        bronze_path = Path(path)
        assert bronze_path.exists(), "Parquet file not created"
        assert bronze_path.suffix == ".parquet"

        sidecar_path = bronze_path.with_suffix(".sha256")
        assert sidecar_path.exists(), "SHA-256 sidecar not created"

    def test_write_returns_correct_path(self, tmp_path):
        """write_bronze_batch returns path in correct format."""
        df = self._create_valid_sensor_df(10)
        bronze_root = tmp_path / "bronze"

        path = write_bronze_batch(df, "sensor_telemetry", bronze_root=bronze_root)

        assert "sensor_telemetry" in path
        assert path.endswith(".parquet")

    def test_checksum_matches_file(self, tmp_path):
        """SHA-256 sidecar matches actual file content."""
        df = self._create_valid_sensor_df(100)
        bronze_root = tmp_path / "bronze"

        path = write_bronze_batch(df, "sensor_telemetry", bronze_root=bronze_root)
        verified = verify_checksum(path)

        assert verified, "Checksum verification failed"

    def test_checksum_fails_on_corruption(self, tmp_path):
        """Checksum verification fails if file is modified."""
        df = self._create_valid_sensor_df(50)
        bronze_root = tmp_path / "bronze"

        path = write_bronze_batch(df, "sensor_telemetry", bronze_root=bronze_root)

        # Corrupt the file
        bronze_path = Path(path)
        content = bronze_path.read_bytes()
        corrupted = bytes([content[0] ^ 0xFF]) + content[1:]
        bronze_path.write_bytes(corrupted)

        verified = verify_checksum(path)
        assert not verified, "Checksum should fail on corrupted file"

    def test_row_count_preserved(self, tmp_path):
        """Written file has same row count as input."""
        n_rows = 500
        df = self._create_valid_sensor_df(n_rows)
        bronze_root = tmp_path / "bronze"

        path = write_bronze_batch(df, "sensor_telemetry", bronze_root=bronze_root)
        read_df = read_bronze_file(path)

        assert len(read_df) == n_rows

    def test_schema_columns_preserved(self, tmp_path):
        """Written file has same columns as input."""
        df = self._create_valid_sensor_df(10)
        bronze_root = tmp_path / "bronze"

        path = write_bronze_batch(df, "sensor_telemetry", bronze_root=bronze_root)
        read_df = read_bronze_file(path)

        assert set(df.columns) == set(read_df.columns)

    def test_partition_date_in_path(self, tmp_path):
        """File is written to correct date partition."""
        df = self._create_valid_sensor_df(10)
        bronze_root = tmp_path / "bronze"

        path = write_bronze_batch(df, "sensor_telemetry", bronze_root=bronze_root)

        # Check that date (YYYY-MM-DD) is in path
        assert any(
            len(segment) == 10 and segment[4] == "-" and segment[7] == "-"
            for segment in Path(path).parts
        ), "Date partition not found in path"

    def test_source_name_in_path(self, tmp_path):
        """File path contains source name."""
        df = self._create_valid_sensor_df(10)
        bronze_root = tmp_path / "bronze"

        path = write_bronze_batch(df, "sensor_telemetry", bronze_root=bronze_root)

        assert "sensor_telemetry" in path

    def test_all_tables_write_correctly(self, tmp_path):
        """All tables can be written to Bronze."""
        tables = ["sensor_telemetry", "mes_lims", "market_prices", "weather", "food_recalls"]

        for table_name in tables:
            df = self._create_df_for_table(table_name, 10)
            bronze_root = tmp_path / "bronze"

            path = write_bronze_batch(df, table_name, bronze_root=bronze_root)

            assert Path(path).exists(), f"Failed to write {table_name}"
            assert verify_checksum(path), f"Checksum failed for {table_name}"

    def test_write_multiple_batches(self, tmp_path):
        """Can write multiple batches without conflict."""
        bronze_root = tmp_path / "bronze"

        paths = []
        for i in range(5):
            df = self._create_valid_sensor_df(100)
            path = write_bronze_batch(df, "sensor_telemetry", bronze_root=bronze_root)
            paths.append(path)

        # All files should exist
        for path in paths:
            assert Path(path).exists()

        # All checksums should verify
        for path in paths:
            assert verify_checksum(path)

    def test_s1_alias_works(self, tmp_path):
        """S1 alias works for backward compatibility."""
        df = self._create_valid_sensor_df(10)
        bronze_root = tmp_path / "bronze"

        path = write_bronze_batch(df, "S1", bronze_root=bronze_root)

        assert Path(path).exists()

    # Helper methods
    def _create_valid_sensor_df(self, n_rows: int) -> pl.DataFrame:
        """Create valid sensor DataFrame for sensor_telemetry."""
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

    def _create_df_for_table(self, table_name: str, n_rows: int) -> pl.DataFrame:
        """Create DataFrame for specified table."""
        if table_name == "sensor_telemetry":
            return self._create_valid_sensor_df(n_rows)
        elif table_name == "mes_lims":
            base_time = datetime(2026, 1, 15, 6, 0, 0)
            return pl.DataFrame({
                "batch_id": [f"BATCH-{i:04d}" for i in range(n_rows)],
                "line_id": ["LINE_UHT_1"] * n_rows,
                "production_start": [base_time.isoformat()] * n_rows,
                "production_end": [(base_time.replace(hour=base_time.hour + 8)).isoformat()] * n_rows,
                "sku_id": ["UHT_PURE"] * n_rows,
                "fat_in": np.random.normal(3.5, 0.2, n_rows).tolist(),
                "protein_in": np.random.normal(3.2, 0.15, n_rows).tolist(),
                "acidity_sh": np.random.uniform(6.5, 7.0, n_rows).tolist(),
                "yield_recovery_pct": np.random.normal(92.5, 1.5, n_rows).tolist(),
                "microbiology_status": ["PASSED"] * n_rows,
                "batch_defect_flag": [0] * n_rows,
                "_source_system": ["TEST"] * n_rows,
                "_generated_at": ["2026-01-15T10:00:00"] * n_rows,
            })
        elif table_name == "market_prices":
            base_time = datetime(2026, 1, 15, 14, 0, 0)
            return pl.DataFrame({
                "source": ["GDT"] * n_rows,
                "product": ["WMP"] * n_rows,
                "contract": ["SPOT"] * n_rows,
                "observed_at": [base_time.isoformat()] * n_rows,
                "price": np.random.uniform(3000, 4000, n_rows).tolist(),
                "currency": ["USD"] * n_rows,
                "unit": ["metric_ton"] * n_rows,
                "ingested_at": [datetime.now(timezone.utc).isoformat()] * n_rows,
                "_source_system": ["TEST"] * n_rows,
                "_generated_at": [datetime.now(timezone.utc).isoformat()] * n_rows,
            })
        elif table_name == "weather":
            base_time = datetime(2026, 1, 15, 10, 0, 0)
            return pl.DataFrame({
                "farm_id": ["MOC_CHAU"] * n_rows,
                "observed_at": [base_time.isoformat()] * n_rows,
                "temperature_c": np.random.normal(28, 5, n_rows).tolist(),
                "relative_humidity_pct": np.random.normal(75, 10, n_rows).tolist(),
                "precipitation_mm": np.abs(np.random.normal(0, 5, n_rows)).tolist(),
                "ingested_at": [datetime.now(timezone.utc).isoformat()] * n_rows,
                "_source_system": ["TEST"] * n_rows,
                "_generated_at": ["2026-01-15T10:00:00"] * n_rows,
            })
        elif table_name == "food_recalls":
            return pl.DataFrame({
                "source": ["FDA"] * n_rows,
                "recall_id": [f"RECALL-{i:04d}" for i in range(n_rows)],
                "published_at": ["2026-01-15T10:00:00"] * n_rows,
                "product": ["Dairy Product"] * n_rows,
                "reason": ["Potential contamination"] * n_rows,
                "status": ["Ongoing"] * n_rows,
                "source_url": ["https://example.com"] * n_rows,
                "ingested_at": [datetime.now(timezone.utc).isoformat()] * n_rows,
                "_source_system": ["TEST"] * n_rows,
                "_generated_at": [datetime.now(timezone.utc).isoformat()] * n_rows,
            })
        else:
            raise ValueError(f"Unknown table: {table_name}")


class TestVerifyChecksum:
    """Test verify_checksum function."""

    def test_verify_valid_file(self, tmp_path):
        """Verification passes for valid file."""
        df = pl.DataFrame({"a": [1, 2, 3], "b": [4, 5, 6]})
        path = tmp_path / "test.parquet"
        df.write_parquet(path)

        # Write correct checksum
        hash_value = hashlib.sha256(path.read_bytes()).hexdigest()
        (path.with_suffix(".sha256")).write_text(hash_value)

        assert verify_checksum(path)

    def test_verify_missing_sidecar(self, tmp_path):
        """Returns False if sidecar is missing."""
        df = pl.DataFrame({"a": [1, 2, 3]})
        path = tmp_path / "test.parquet"
        df.write_parquet(path)

        # No sidecar written
        result = verify_checksum(path)

        assert result is False

    def test_verify_missing_parquet(self):
        """Raises FileNotFoundError if Parquet is missing."""
        with pytest.raises(FileNotFoundError):
            verify_checksum("/nonexistent/file.parquet")


class TestListBronzeFiles:
    """Test list_bronze_files function."""

    def test_list_returns_all_files(self, tmp_path):
        """Returns all Parquet files in source directory."""
        bronze_root = tmp_path / "bronze"

        # Write files for different tables
        df1 = pl.DataFrame({"a": [1]})
        df2 = pl.DataFrame({"b": [2]})

        write_bronze_batch(df1, "sensor_telemetry", bronze_root=bronze_root, validate_schema=False)
        write_bronze_batch(df2, "sensor_telemetry", bronze_root=bronze_root, validate_schema=False)

        files = list_bronze_files("sensor_telemetry", bronze_root=bronze_root)

        assert len(files) == 2
        assert all(f.suffix == ".parquet" for f in files)

    def test_list_filters_by_date(self, tmp_path):
        """Can filter files by date range."""
        bronze_root = tmp_path / "bronze"

        # Write file
        df = pl.DataFrame({"a": [1]})
        write_bronze_batch(df, "sensor_telemetry", bronze_root=bronze_root, validate_schema=False)

        # List with date filter
        files = list_bronze_files(
            "sensor_telemetry",
            bronze_root=bronze_root,
            date_from="2026-01-01",
            date_to="2026-12-31",
        )

        assert len(files) >= 1

    def test_list_empty_for_nonexistent_table(self, tmp_path):
        """Returns empty list for nonexistent table."""
        bronze_root = tmp_path / "bronze"
        bronze_root.mkdir(parents=True)

        files = list_bronze_files("sensor_telemetry", bronze_root=bronze_root)

        assert files == []


class TestReadBronzeFile:
    """Test read_bronze_file function."""

    def test_read_parquet_file(self, tmp_path):
        """Can read back written Parquet file."""
        df = pl.DataFrame({
            "id": [1, 2, 3],
            "value": ["a", "b", "c"],
        })
        path = tmp_path / "test.parquet"
        df.write_parquet(path)

        read_df = read_bronze_file(path)

        assert read_df.equals(df)


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
