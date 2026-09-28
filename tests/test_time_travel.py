"""Tests for time_travel module."""

from datetime import date, datetime, timedelta, timezone
from pathlib import Path

import numpy as np
import polars as pl
import pytest


class TestTimeTravel:
    """Test time-travel queries."""

    @pytest.fixture
    def delta_table(self, tmp_path):
        """Create a Delta table with multiple versions."""
        from lakehouse_storage.silver_manager import append_to_delta

        table_uri = str(tmp_path / "silver" / "test_timetravel")

        # Create version 0
        df1 = self._create_sensor_df(100, batch_start=0)
        append_to_delta(df1, table_uri, partition_by=["line_id", "ingestion_date"], table_name="sensor_telemetry")

        # Create version 1
        df2 = self._create_sensor_df(50, batch_start=100)
        append_to_delta(df2, table_uri, partition_by=["line_id", "ingestion_date"], table_name="sensor_telemetry")

        # Create version 2
        df3 = self._create_sensor_df(30, batch_start=150)
        append_to_delta(df3, table_uri, partition_by=["line_id", "ingestion_date"], table_name="sensor_telemetry")

        return table_uri

    def test_load_version_0(self, delta_table):
        """Can load version 0 (first commit)."""
        from lakehouse_storage.time_travel import load_version

        df = load_version(delta_table, version=0)

        assert len(df) == 100
        assert "timestamp" in df.columns

    def test_load_version_1(self, delta_table):
        """Can load version 1."""
        from lakehouse_storage.time_travel import load_version

        df = load_version(delta_table, version=1)

        # Version 1 has data from version 0 + 50 new rows
        assert len(df) == 150

    def test_load_version_2(self, delta_table):
        """Can load version 2."""
        from lakehouse_storage.time_travel import load_version

        df = load_version(delta_table, version=2)

        # Version 2 has all data
        assert len(df) == 180

    def test_get_table_history(self, delta_table):
        """Can get table commit history."""
        from lakehouse_storage.time_travel import get_table_history

        history = get_table_history(delta_table)

        assert len(history) >= 3  # At least 3 commits

        # Should have version column
        assert "version" in history.columns

        # Versions should be in descending order (newest first)
        versions = history["version"].to_list()
        assert versions == sorted(versions, reverse=True)

    def test_diff_versions(self, delta_table):
        """Can diff two versions."""
        from lakehouse_storage.time_travel import diff_versions

        result = diff_versions(delta_table, v1=0, v2=1)

        # Should have 50 new rows in v1 not in v0
        assert result["counts"]["added_count"] == 50
        assert result["counts"]["removed_count"] == 0

    def test_diff_versions_empty_if_same(self, delta_table):
        """Diff returns empty if versions are identical."""
        from lakehouse_storage.time_travel import diff_versions

        result = diff_versions(delta_table, v1=0, v2=0)

        assert result["counts"]["added_count"] == 0
        assert result["counts"]["removed_count"] == 0

    def test_get_latest_version(self, delta_table):
        """Can get latest version number."""
        from lakehouse_storage.time_travel import get_latest_version

        latest = get_latest_version(delta_table)

        assert latest == 2

    def test_load_as_of_timestamp(self, delta_table):
        """Can load table as of a timestamp."""
        from lakehouse_storage.time_travel import get_table_history, load_as_of

        # Get timestamp from history
        history = get_table_history(delta_table)
        if len(history) > 1:
            # Get timestamp from version 1
            ts = history.filter(pl.col("version") == 1)["timestamp"].to_list()[0]

            # Load as of that timestamp
            df = load_as_of(delta_table, timestamp_iso=ts)

            # Should have data from version 0 and 1
            assert len(df) >= 100

    def test_load_as_of_invalid_timestamp(self, delta_table):
        """Raises error for invalid timestamp format."""
        from lakehouse_storage.time_travel import load_as_of

        with pytest.raises(ValueError, match="Invalid timestamp"):
            load_as_of(delta_table, timestamp_iso="not-a-timestamp")

    def test_generate_audit_report(self, delta_table, tmp_path):
        """Can generate audit report for date range."""
        from lakehouse_storage.time_travel import generate_audit_report

        report = generate_audit_report(
            delta_table,
            date_from="2026-01-01",
            date_to="2026-12-31",
        )

        assert len(report) > 0
        assert "_bronze_file" in report.columns
        assert "_bronze_sha256" in report.columns

    def test_generate_audit_report_to_file(self, delta_table, tmp_path):
        """Can save audit report to Parquet."""
        from lakehouse_storage.time_travel import generate_audit_report

        output_path = tmp_path / "audit_report.parquet"

        report = generate_audit_report(
            delta_table,
            date_from="2026-01-01",
            date_to="2026-12-31",
            output_path=output_path,
        )

        assert output_path.exists()

    # Helper methods
    def _create_sensor_df(self, n_rows: int, batch_start: int = 0) -> pl.DataFrame:
        """Create sensor_telemetry DataFrame with all required columns."""
        base_time = datetime(2026, 1, 15, 10, 0, 0)
        timestamps = [
            base_time + timedelta(seconds=batch_start + i) for i in range(n_rows)
        ]

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
            "ingestion_date": [date(2026, 1, 15)] * n_rows,
            "_source_system": ["TEST"] * n_rows,
            "_bronze_file": ["/test/path.parquet"] * n_rows,
            "_bronze_sha256": ["abc123def456"] * n_rows,
            "_ingested_at": [datetime.now(timezone.utc).replace(tzinfo=None)] * n_rows,
        })


class TestTraceability:
    """Test ISO 22000 traceability functions."""

    @pytest.fixture
    def traced_table(self, tmp_path):
        """Create Delta table with lineage data."""
        from lakehouse_storage.silver_manager import append_to_delta

        table_uri = str(tmp_path / "silver" / "trace_table")

        df = self._create_sensor_df_with_lineage(50)
        append_to_delta(df, table_uri, partition_by=["line_id", "ingestion_date"], table_name="sensor_telemetry")

        return table_uri

    def test_trace_event(self, traced_table):
        """Can trace event to its source."""
        from lakehouse_storage.time_travel import trace_event

        # Trace first event (using timestamp)
        first_ts = datetime(2026, 1, 15, 10, 0, 0)
        result = trace_event(traced_table, event_id=str(first_ts))

        assert "lineage" in result
        assert "bronze_file" in result["lineage"]
        assert "bronze_sha256" in result["lineage"]

    def test_trace_event_not_found(self, traced_table):
        """Raises error if event not found."""
        from lakehouse_storage.time_travel import trace_event

        with pytest.raises(ValueError, match="Batch not found"):
            trace_event(traced_table, event_id="nonexistent-event")

    # Helper methods
    def _create_sensor_df_with_lineage(self, n_rows: int) -> pl.DataFrame:
        """Create sensor_telemetry DataFrame with lineage columns."""
        timestamps = [datetime(2026, 1, 15, 10, i % 60, i % 60) for i in range(n_rows)]

        bronze_files = [
            f"/data/01_bronze_vault/sensor_telemetry/2026-01-15/sensor_telemetry_{i:010d}.parquet"
            for i in range(n_rows)
        ]

        sha256s = [f"{i:064d}" for i in range(n_rows)]

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
            "ingestion_date": [date(2026, 1, 15)] * n_rows,
            "_source_system": ["TEST"] * n_rows,
            "_bronze_file": bronze_files,
            "_bronze_sha256": sha256s,
            "_ingested_at": [datetime.now(timezone.utc).replace(tzinfo=None)] * n_rows,
        })


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
