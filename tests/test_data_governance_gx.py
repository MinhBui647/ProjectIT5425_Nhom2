"""Great Expectations suite, checkpoint, and Data Docs integration tests."""

from pathlib import Path

import great_expectations as gx
import polars as pl

from data_governance.bronze_loader import load_bronze
from data_governance.contracts import CONTRACTS, validate_table
from data_governance.gx_context import create_gx_context, run_gx_checkpoint
from data_governance.transformers import transform_bronze


FIXTURE_ROOT = Path(__file__).parent / "fixtures" / "bronze"


def _canonical_fixture(table_name):
    path = next((FIXTURE_ROOT / table_name).glob("*.parquet"))
    bronze, _ = load_bronze(path, FIXTURE_ROOT)
    return transform_bronze(bronze, table_name)


def test_gx_clean_and_failing_results_match_operational_validator(tmp_path):
    docs_path = tmp_path / "reports" / "data_docs"
    context = create_gx_context(
        project_root=tmp_path,
        data_docs_path=docs_path,
    )

    assert gx.__version__.startswith("1.")
    assert {suite.name for suite in context.suites.all()} == {
        f"governance_{table_name}_v1" for table_name in CONTRACTS
    }

    for table_name in CONTRACTS:
        frame = _canonical_fixture(table_name)
        operational = validate_table(frame, table_name)
        gx_result = run_gx_checkpoint(context, frame, table_name)
        assert not operational.hard_failures
        assert operational.invalid_row_count == 0
        assert gx_result["success"] is True

    bad_sensor = _canonical_fixture("sensor_telemetry").with_columns(
        pl.when(pl.int_range(pl.len()) == 0)
        .then(pl.lit(200.0))
        .otherwise(pl.col("uht_temp"))
        .cast(pl.Float32)
        .alias("uht_temp")
    )
    operational_bad = validate_table(bad_sensor, "sensor_telemetry")
    gx_bad = run_gx_checkpoint(context, bad_sensor, "sensor_telemetry")

    assert operational_bad.invalid_row_count == 1
    assert gx_bad["success"] is False
    assert (docs_path / "index.html").is_file()
    assert (docs_path / "index.html").stat().st_size > 0
    assert len(list(docs_path.rglob("*.html"))) >= 2

