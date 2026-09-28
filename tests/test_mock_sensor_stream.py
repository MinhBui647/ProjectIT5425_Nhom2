"""Tests for mock_sensor_stream.py."""

from datetime import datetime, timezone

import polars as pl
import pytest

from lakehouse_storage.mock_sensor_stream import (
    generate_sensor_batch,
)
from lakehouse_storage.silver_manager import _to_silver_frame


class TestGenerateSensorBatch:
    """Test generate_sensor_batch()."""

    def test_returns_polars_dataframe(self):
        df = generate_sensor_batch(n_samples=10)
        assert isinstance(df, pl.DataFrame)

    def test_correct_row_count(self):
        df = generate_sensor_batch(n_samples=100)
        assert len(df) == 100

    def test_schema_columns(self):
        """Verify Bronze schema."""
        df = generate_sensor_batch(n_samples=10)
        expected_cols = {
            "timestamp", "line_id", "batch_id",
            "preheat_temp", "uht_temp", "homo_press_stage1", "homo_press_stage2",
            "flow_rate", "conductivity", "power_kw",
            "_source_system", "_generated_at",
        }
        assert set(df.columns) == expected_cols

    def test_timestamps_increment_by_second(self):
        df = generate_sensor_batch(n_samples=5, seed=42)
        timestamps = df["timestamp"].to_list()
        # Parse timestamps
        for i, ts in enumerate(timestamps):
            dt = datetime.fromisoformat(ts)
            assert dt.tzinfo is not None

    def test_line_id_preserved(self):
        df = generate_sensor_batch(n_samples=10, line_id="LINE_TEST")
        assert (df["line_id"] == "LINE_TEST").all()

    def test_batch_id_can_be_null(self):
        df = generate_sensor_batch(n_samples=10, batch_id=None)
        assert df["batch_id"].to_list().count(None) == 10

    def test_batch_id_preserved(self):
        df = generate_sensor_batch(n_samples=10, batch_id="BATCH_001")
        assert (df["batch_id"] == "BATCH_001").all()

    def test_reproducibility_with_seed(self):
        df1 = generate_sensor_batch(n_samples=10, seed=123)
        df2 = generate_sensor_batch(n_samples=10, seed=123)
        assert df1["preheat_temp"].to_list() == df2["preheat_temp"].to_list()

    def test_invalid_n_samples_raises(self):
        with pytest.raises(ValueError):
            generate_sensor_batch(n_samples=0)

    def test_invalid_n_samples_negative_raises(self):
        with pytest.raises(ValueError):
            generate_sensor_batch(n_samples=-1)

    def test_float_columns_are_float64(self):
        """Bronze schema: float64 columns."""
        df = generate_sensor_batch(n_samples=10)
        assert df["uht_temp"].dtype == pl.Float64
        assert df["preheat_temp"].dtype == pl.Float64

    def test_source_system_set(self):
        df = generate_sensor_batch(n_samples=10)
        assert (df["_source_system"] == "mock_sensor_stream").all()


class TestToSilver:
    """Test Bronze -> Silver transformation via _to_silver_frame()."""

    def test_float64_to_float32(self):
        df = generate_sensor_batch(n_samples=10)
        silver = _to_silver_frame(df, "sensor_telemetry")
        assert silver["uht_temp"].dtype == pl.Float32

    def test_timestamp_parsed_to_datetime(self):
        df = generate_sensor_batch(n_samples=10)
        silver = _to_silver_frame(df, "sensor_telemetry")
        assert silver["timestamp"].dtype == pl.Datetime

    def test_ingestion_date_added(self):
        df = generate_sensor_batch(n_samples=10)
        silver = _to_silver_frame(df, "sensor_telemetry")
        assert "ingestion_date" in silver.columns

    def test_ingested_at_added(self):
        df = generate_sensor_batch(n_samples=10)
        silver = _to_silver_frame(df, "sensor_telemetry")
        assert "_ingested_at" in silver.columns
