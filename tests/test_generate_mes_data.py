"""Tests for generate_mes_data.py."""

from datetime import datetime, timezone, date

import polars as pl
import pytest

from lakehouse_storage.generate_mes_data import (
    generate_mes_batch,
    generate_daily_production,
)
from lakehouse_storage.silver_manager import _to_silver_frame


class TestGenerateMesBatch:
    """Test generate_mes_batch()."""

    def test_returns_polars_dataframe(self):
        df = generate_mes_batch(date=datetime.now(timezone.utc))
        assert isinstance(df, pl.DataFrame)

    def test_single_row(self):
        df = generate_mes_batch(date=datetime.now(timezone.utc))
        assert len(df) == 1

    def test_schema_columns(self):
        """Verify Bronze schema."""
        df = generate_mes_batch(date=datetime.now(timezone.utc))
        expected_cols = {
            "batch_id", "line_id", "production_start", "production_end",
            "sku_id", "fat_in", "protein_in", "acidity_sh",
            "yield_recovery_pct", "microbiology_status", "batch_defect_flag",
            "_source_system", "_generated_at",
        }
        assert set(df.columns) == expected_cols

    def test_batch_id_format(self):
        df = generate_mes_batch(
            date=datetime(2025, 1, 15),
            line_id="LINE_UHT_1",
            batch_id="BATCH_TEST",
        )
        assert df["batch_id"][0] == "BATCH_TEST"

    def test_defect_flag_zero_when_no_defect(self):
        df = generate_mes_batch(
            date=datetime.now(timezone.utc),
            include_defect=False,
        )
        assert df["batch_defect_flag"][0] == 0

    def test_defect_flag_one_when_defect(self):
        df = generate_mes_batch(
            date=datetime.now(timezone.utc),
            include_defect=True,
        )
        assert df["batch_defect_flag"][0] == 1

    def test_microbiology_status_failed_when_defect(self):
        df = generate_mes_batch(
            date=datetime.now(timezone.utc),
            include_defect=True,
        )
        assert df["microbiology_status"][0] == "FAILED"

    def test_production_times_are_strings(self):
        """Bronze: timestamps as strings."""
        df = generate_mes_batch(date=datetime.now(timezone.utc))
        assert df["production_start"].dtype == pl.String
        assert df["production_end"].dtype == pl.String

    def test_reproducibility_with_seed(self):
        d = datetime(2025, 1, 15, tzinfo=timezone.utc)
        df1 = generate_mes_batch(date=d, seed=42)
        df2 = generate_mes_batch(date=d, seed=42)
        assert df1["fat_in"][0] == df2["fat_in"][0]


class TestGenerateDailyProduction:
    """Test generate_daily_production()."""

    def test_returns_tuple(self):
        result = generate_daily_production(date=datetime.now(timezone.utc))
        assert isinstance(result, tuple)
        assert len(result) == 3

    def test_sensor_28800_rows(self):
        """8 hours x 3600 samples/hour = 28800 rows."""
        sensor_df, mes_df, batch_farm_map = generate_daily_production(
            date=datetime.now(timezone.utc),
            seed=42,
        )
        assert len(sensor_df) == 28800

    def test_mes_4_rows(self):
        """4 batches per day."""
        sensor_df, mes_df, batch_farm_map = generate_daily_production(
            date=datetime.now(timezone.utc),
            seed=42,
        )
        assert len(mes_df) == 4

    def test_batch_farm_map_4_entries(self):
        sensor_df, mes_df, batch_farm_map = generate_daily_production(
            date=datetime.now(timezone.utc),
            seed=42,
        )
        assert len(batch_farm_map) == 4

    def test_batch_ids_in_sensor_match_mes(self):
        sensor_df, mes_df, batch_farm_map = generate_daily_production(
            date=datetime.now(timezone.utc),
            seed=42,
        )
        sensor_batches = set(sensor_df["batch_id"].unique())
        mes_batches = set(mes_df["batch_id"])
        assert sensor_batches == mes_batches

    def test_defect_rate_applied(self):
        """Test with defect_rate=1.0 (100% defective)."""
        sensor_df, mes_df, batch_farm_map = generate_daily_production(
            date=datetime.now(timezone.utc),
            seed=42,
            defect_rate=1.0,
        )
        # All batches have batch_defect_flag=1
        assert (mes_df["batch_defect_flag"] == 1).all()

    def test_no_defects_with_zero_rate(self):
        sensor_df, mes_df, batch_farm_map = generate_daily_production(
            date=datetime.now(timezone.utc),
            seed=42,
            defect_rate=0.0,
        )
        # May have 0 or 1 defect (if random picks FAILED)
        # Ensure no FAILED in microbiology status
        failed = mes_df.filter(pl.col("microbiology_status") == "FAILED")
        # With a fixed seed, the result is reproducible
        assert len(failed) == 0


class TestToSilverMes:
    """Test Bronze -> Silver transformation via _to_silver_frame()."""

    def test_float64_to_float32(self):
        df = generate_mes_batch(date=datetime.now(timezone.utc), seed=42)
        silver = _to_silver_frame(df, "mes_lims")
        assert silver["fat_in"].dtype == pl.Float32
        assert silver["protein_in"].dtype == pl.Float32

    def test_batch_defect_flag_to_int8(self):
        df = generate_mes_batch(date=datetime.now(timezone.utc))
        silver = _to_silver_frame(df, "mes_lims")
        assert silver["batch_defect_flag"].dtype == pl.Int8

    def test_timestamps_parsed_to_datetime(self):
        df = generate_mes_batch(date=datetime.now(timezone.utc), seed=42)
        silver = _to_silver_frame(df, "mes_lims")
        assert silver["production_start"].dtype == pl.Datetime
        assert silver["production_end"].dtype == pl.Datetime

    def test_ingestion_date_added(self):
        df = generate_mes_batch(date=datetime.now(timezone.utc), seed=42)
        silver = _to_silver_frame(df, "mes_lims")
        assert "ingestion_date" in silver.columns

    def test_ingested_at_added(self):
        df = generate_mes_batch(date=datetime.now(timezone.utc), seed=42)
        silver = _to_silver_frame(df, "mes_lims")
        assert "_ingested_at" in silver.columns
