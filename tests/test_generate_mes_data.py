"""Tests for generate_mes_data.py."""

from datetime import datetime, timezone, date, timedelta

import numpy as np
import polars as pl
import pytest

from lakehouse_storage.generate_mes_data import (
    generate_mes_batch,
    generate_daily_production,
)
from lakehouse_storage.silver_manager import _to_silver_frame
import lakehouse_storage.generate_mes_data as generator


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


class TestSeedStreams:
    @staticmethod
    def days(count=2, lines=("LINE_UHT_1",)):
        rows = []
        for day in range(count):
            for line in lines:
                sensor, mes, _ = generate_daily_production(
                    datetime(2025, 1, 1) + timedelta(days=day), line, seed=5425 + day,
                )
                rows.append(mes.join(
                    sensor.group_by("batch_id").agg(pl.col("uht_temp").mean()),
                    on="batch_id",
                ))
        return pl.concat(rows)

    def test_batches_do_not_repeat_between_days(self):
        mes = self.days()
        assert mes.select("fat_in", "protein_in", "acidity_sh", "yield_recovery_pct").unique().height == mes.height

    def test_lines_have_independent_values(self):
        mes = self.days(lines=("LINE_UHT_1", "LINE_UHT_2"))
        assert mes.select("fat_in", "protein_in", "acidity_sh").unique().height == mes.height

    def test_mes_does_not_copy_sensor_drift(self):
        mes = self.days(count=8)
        assert abs(np.corrcoef(mes["fat_in"], mes["uht_temp"])[0, 1]) < 0.35

    def test_seed_zero_reproduces_business_values(self):
        first = generate_daily_production(datetime(2025, 1, 1), seed=0)
        second = generate_daily_production(datetime(2025, 1, 1), seed=0)
        for left, right in zip(first[:2], second[:2]):
            assert left.drop("_generated_at").equals(right.drop("_generated_at"))
        assert first[2] == second[2]


class TestMesMechanism:
    def test_defect_probability_direction_and_calibration(self):
        probability = generator.defect_probability
        assert probability(139.7, 6.5) > probability(140, 6.5) > probability(140.3, 6.5)
        assert probability(140, 6.9) > probability(140, 6.5)
        rng = np.random.default_rng(0)
        values = [probability(140 + drift, acidity) for drift, acidity in zip(
            rng.normal(0, 0.1, 20000), rng.normal(6.5, 0.2, 20000),
        )]
        assert np.mean(values) == pytest.approx(0.05, abs=0.005)

    @pytest.mark.parametrize("rate", [0.0, 1.0])
    def test_sampled_defect_endpoints(self, rate):
        mes = generate_mes_batch(datetime(2025, 1, 1), seed=42, include_defect=None,
                                 uht_level=140, defect_rate=rate)
        assert mes["batch_defect_flag"][0] == int(rate)
        assert generator.defect_probability(140, 6.5, rate) == rate

    @pytest.mark.parametrize("override,difference", [
        ({"farm_id": "CU_CHI"}, -1.28),
        ({"line_id": "LINE_UHT_2"}, -0.8),
        ({"include_defect": True}, -6.0),
        ({"uht_level": 141.0}, -0.5),
    ])
    def test_yield_responds_to_controlled_inputs(self, override, difference):
        args = dict(date=datetime(2025, 1, 1), seed=42, farm_id="MOC_CHAU",
                    uht_level=140, line_id="LINE_UHT_1", include_defect=False)
        base = generate_mes_batch(**args)
        changed = generate_mes_batch(**{**args, **override})
        assert changed["yield_recovery_pct"][0] - base["yield_recovery_pct"][0] == pytest.approx(difference)

    def test_fat_and_protein_yield_coefficients(self):
        mes = pl.concat([generate_mes_batch(datetime(2025, 1, 1), seed=i) for i in range(2000)])
        design = np.column_stack([np.ones(mes.height), mes["fat_in"], mes["protein_in"]])
        coefficients = np.linalg.lstsq(design, mes["yield_recovery_pct"].to_numpy(), rcond=None)[0]
        assert coefficients[1] == pytest.approx(8.0, abs=0.8)
        assert coefficients[2] == pytest.approx(5.0, abs=1.5)

    def test_daily_defects_use_the_batch_uht_level(self, monkeypatch):
        levels = []
        probability = generator.defect_probability

        def record(level, acidity, defect_rate):
            levels.append(level)
            return probability(level, acidity, defect_rate)

        monkeypatch.setattr(generator, "defect_probability", record)
        sensor, mes, _ = generate_daily_production(datetime(2025, 1, 1), seed=5425)
        observed = sensor.group_by("batch_id").agg(pl.col("uht_temp").mean()).sort("batch_id")
        assert len(levels) == mes.height
        assert np.allclose(levels, observed["uht_temp"].to_numpy(), atol=0.03)


class TestSensorMechanism:
    @staticmethod
    def sensor(seed=42, sku="UHT_PURE", samples=200):
        return generator._generate_sensor_batch_for_mes(
            samples, "LINE_UHT_1", "BATCH_TEST", datetime(2025, 1, 1, tzinfo=timezone.utc),
            seed=seed, sku_id=sku,
        )

    @pytest.mark.parametrize("sku,offset", [("UHT_SWEETENED", 3.0), ("UHT_LOWFAT", -2.0)])
    def test_sku_changes_power_without_changing_flow(self, sku, offset):
        base, changed = self.sensor(), self.sensor(sku=sku)
        assert base["flow_rate"].equals(changed["flow_rate"])
        assert np.allclose(changed["power_kw"] - base["power_kw"], offset)

    def test_flow_and_power_follow_batch_levels(self):
        levels = np.array([
            [frame["flow_rate"].mean(), frame["power_kw"].mean()]
            for frame in [self.sensor(seed=seed) for seed in range(40)]
        ])
        assert np.std(levels[:, 0]) > 100
        assert np.corrcoef(levels.T)[0, 1] > 0.8
        assert np.polyfit(levels[:, 0], levels[:, 1], 1)[0] == pytest.approx(0.01, abs=0.002)


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
