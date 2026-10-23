"""Headless smoke tests for the three mandatory report figures."""

from datetime import datetime, timedelta, timezone

import polars as pl
from PIL import Image

from data_governance.visualization import generate_required_figures


def test_generate_three_parseable_non_empty_png_figures(tmp_path):
    quality_df = pl.DataFrame(
        {
            "farm_id": ["MOC_CHAU", "MOC_CHAU", None, "MOC_CHAU"],
            "temperature_c": [25.0, None, 30.0, 31.0],
            "humidity": [80.0, 75.0, None, 70.0],
        }
    )
    base = datetime(2026, 1, 1, tzinfo=timezone.utc)
    values = [140.0] * 21
    values[10] = 160.0
    sensor_df = pl.DataFrame(
        {
            "timestamp": [base + timedelta(seconds=index) for index in range(21)],
            "uht_temp": pl.Series(values, dtype=pl.Float32),
            "is_anomaly_flag": [index == 10 for index in range(21)],
        }
    ).with_columns(pl.col("timestamp").cast(pl.Datetime("us", "UTC")))
    metadata = {
        "run_id": "run-figures",
        "bronze_file": "data/01_bronze_vault/sensor.parquet",
        "silver_uri": "data/02_silver_curated/sensor_telemetry",
        "data_docs": "reports/data_docs/index.html",
    }

    paths = generate_required_figures(
        quality_df,
        sensor_df,
        metadata,
        run_id="run-figures",
        figures_root=tmp_path,
    )

    assert set(paths) == {
        "missingness_heatmap",
        "sensor_physical_bands",
        "lineage_dag",
    }
    for path in paths.values():
        assert path.is_file()
        assert path.stat().st_size > 2_000
        with Image.open(path) as image:
            image.verify()
        with Image.open(path) as image:
            assert image.format == "PNG"
            assert image.size == (1200, 675)

