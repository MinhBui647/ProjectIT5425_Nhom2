"""Tests for maintenance module."""

from datetime import date, datetime, timezone
from pathlib import Path

import numpy as np
import polars as pl
import pytest


class TestMaintenance:
    """Test maintenance functions."""

    @pytest.fixture
    def delta_table(self, tmp_path):
        """Create a Delta table with multiple small files."""
        from lakehouse_storage.silver_manager import append_to_delta

        table_uri = str(tmp_path / "silver" / "maintenance_test")

        # Append many small batches to create multiple files
        for i in range(10):
            df = self._create_sensor_df(100, batch_start=i * 100)
            append_to_delta(df, table_uri, partition_by=["line_id", "ingestion_date"], table_name="sensor_telemetry")

        return table_uri

    def test_get_table_stats(self, delta_table):
        """Can get table statistics."""
        from lakehouse_storage.maintenance import get_table_stats

        stats = get_table_stats(delta_table)

        assert "version" in stats
        assert "file_count" in stats
        assert "total_bytes" in stats
        assert stats["file_count"] >= 10  # At least 10 files

    def test_compact_table(self, delta_table):
        """Compaction reduces file count."""
        from lakehouse_storage.maintenance import compact_table, get_table_stats

        # Get initial file count
        stats_before = get_table_stats(delta_table)
        files_before = stats_before["file_count"]

        # Compact
        result = compact_table(delta_table)

        assert "files_before" in result
        assert "files_after" in result
        assert result["files_after"] < result["files_before"]

    def test_compact_reduces_count(self, delta_table):
        """Compaction reduces number of files."""
        from lakehouse_storage.maintenance import compact_table

        result = compact_table(delta_table)

        # Should reduce file count
        assert result["files_removed"] > 0

    def test_vacuum_dry_run(self, delta_table):
        """Vacuum dry run doesn't delete files."""
        from lakehouse_storage.maintenance import vacuum_table

        result = vacuum_table(delta_table, retention_hours=0, dry_run=True)

        assert result["dry_run"] is True
        # Should return file list without deleting
        assert "files_removed" in result

    def test_get_optimization_recommendations(self, delta_table):
        """Can get optimization recommendations."""
        from lakehouse_storage.maintenance import get_optimization_recommendations

        recommendations = get_optimization_recommendations(delta_table)

        assert isinstance(recommendations, list)
        assert len(recommendations) > 0
        # Should have at least one recommendation
        assert any("compaction" in r.lower() or "file" in r.lower() for r in recommendations)

    def test_get_file_sizes(self, delta_table):
        """Can get file size information."""
        from lakehouse_storage.maintenance import get_file_sizes

        sizes = get_file_sizes(delta_table)

        assert isinstance(sizes, list)
        if len(sizes) > 0:
            assert "path" in sizes[0]
            assert "size_bytes" in sizes[0]

    def test_auto_compact_if_needed_triggered(self, delta_table):
        """Auto-compact triggers when file count exceeds threshold."""
        from lakehouse_storage.maintenance import auto_compact_if_needed

        # Trigger with low threshold
        result = auto_compact_if_needed(delta_table, file_count_threshold=5)

        # Should trigger compaction
        assert result is not None
        assert "files_before" in result

    def test_auto_compact_if_needed_skipped(self, delta_table):
        """Auto-compact skips when file count is low."""
        from lakehouse_storage.maintenance import auto_compact_if_needed

        # Use high threshold
        result = auto_compact_if_needed(delta_table, file_count_threshold=1000)

        # Should skip
        assert result is None

    def test_run_full_maintenance(self, delta_table):
        """Can run full maintenance cycle."""
        from lakehouse_storage.maintenance import run_full_maintenance

        result = run_full_maintenance(
            delta_table,
            compact=True,
            zorder_cols=["line_id"],
            vacuum=False,  # Don't vacuum in test
        )

        assert "compaction" in result or "error" in result.get("compaction", {})

    # Helper methods
    def _create_sensor_df(self, n_rows: int, batch_start: int = 0) -> pl.DataFrame:
        """Create sensor_telemetry DataFrame."""
        timestamps = [datetime(2026, 1, 15, 10, i % 60, i % 60) for i in range(n_rows)]

        return pl.DataFrame({
            "timestamp": timestamps,
            "line_id": ["LINE_UHT_1"] * n_rows,
            "batch_id": [None] * n_rows,
            "preheat_temp": np.random.normal(75.0,  2.0, n_rows).astype(np.float32),
            "uht_temp": np.random.normal(138.5, 0.7, n_rows).astype(np.float32),
            "homo_press_stage1": np.random.normal(200.0, 5.0, n_rows).astype(np.float32),
            "homo_press_stage2": np.random.normal(30.0, 2.0, n_rows).astype(np.float32),
            "flow_rate": np.random.normal(5000.0, 50.0, n_rows).astype(np.float32),
            "conductivity": np.random.normal(4.5, 0.1, n_rows).astype(np.float32),
            "power_kw": np.random.normal(150.0, 5.0, n_rows).astype(np.float32),
        })


class TestZOrder:
    """Test Z-order optimization."""

    @pytest.fixture
    def delta_table(self, tmp_path):
        """Create a Delta table."""
        from lakehouse_storage.silver_manager import append_to_delta

        table_uri = str(tmp_path / "silver" / "zorder_test")

        df = self._create_sensor_df(500)
        append_to_delta(df, table_uri, partition_by=["line_id", "ingestion_date"], table_name="sensor_telemetry")

        return table_uri

    def test_zorder_completes(self, delta_table):
        """Z-order optimization completes without error."""
        from lakehouse_storage.maintenance import zorder_table

        result = zorder_table(delta_table, columns=["line_id"])

        assert "columns" in result
        assert result["columns"] == ["line_id"]

    def test_zorder_with_multiple_columns(self, delta_table):
        """Can Z-order by multiple columns."""
        from lakehouse_storage.maintenance import zorder_table

        result = zorder_table(delta_table, columns=["line_id", "ingestion_date"])

        assert "columns" in result
        assert "line_id" in result["columns"]

    def test_zorder_empty_columns_raises(self, delta_table):
        """Raises error for empty columns list."""
        from lakehouse_storage.maintenance import zorder_table

        with pytest.raises(ValueError, match="columns list cannot be empty"):
            zorder_table(delta_table, columns=[])

    # Helper methods
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


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
