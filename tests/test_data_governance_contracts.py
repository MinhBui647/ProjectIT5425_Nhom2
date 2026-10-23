"""Tests for the generic governance rule engine and table contracts."""

import json
from datetime import datetime, timedelta, timezone
from pathlib import Path

import polars as pl
import pytest

from data_governance.validator import (
    DamaDimension,
    RowRule,
    RuleSeverity,
    TableRule,
    TableRuleOutcome,
    evaluate_rules,
)
from data_governance.bronze_loader import load_bronze
from data_governance.contracts import validate_table
from data_governance.transformers import transform_bronze


FIXTURE_ROOT = Path(__file__).parent / "fixtures" / "bronze"


def _canonical_fixture(table_name: str) -> pl.DataFrame:
    path = next((FIXTURE_ROOT / table_name).glob("*.parquet"))
    bronze, _ = load_bronze(path, FIXTURE_ROOT)
    return transform_bronze(bronze, table_name)


def test_rule_engine_merges_overlapping_row_failures_once():
    frame = pl.DataFrame({"value": [-1, 0, 2]})
    rules = (
        RowRule(
            rule_id="demo.non_positive.v1",
            dimension=DamaDimension.VALIDITY,
            severity=RuleSeverity.ERROR,
            description="Value must be positive",
            failure_expression=lambda _: pl.col("value") <= 0,
        ),
        RowRule(
            rule_id="demo.even.v1",
            dimension=DamaDimension.CONSISTENCY,
            severity=RuleSeverity.ERROR,
            description="Value must not be even",
            failure_expression=lambda _: pl.col("value") % 2 == 0,
        ),
    )

    result = evaluate_rules(frame, row_rules=rules)

    assert result.invalid_row_mask == (True, True, True)
    assert result.invalid_row_count == 3
    assert result.failed_rules_for_row(1) == (
        "demo.non_positive.v1",
        "demo.even.v1",
    )
    assert result.rule_metrics["demo.non_positive.v1"]["failed_count"] == 2
    assert result.rule_metrics["demo.even.v1"]["failed_count"] == 2


def test_table_hard_failure_is_separate_from_row_error_ratio_and_serializes():
    frame = pl.DataFrame({"value": [1, 2]})
    rule = TableRule(
        rule_id="demo.schema.v1",
        dimension=DamaDimension.COMPLETENESS,
        severity=RuleSeverity.ERROR,
        description="Required columns must exist",
        evaluator=lambda _: TableRuleOutcome(
            passed=False,
            observed={"missing_columns": ["required"]},
            message="Missing required column: required",
        ),
        hard_failure=True,
    )

    result = evaluate_rules(frame, table_rules=(rule,))
    payload = json.loads(result.to_json())

    assert result.invalid_row_count == 0
    assert result.error_ratio == 0.0
    assert result.hard_failures == ("demo.schema.v1",)
    assert payload["hard_failures"] == ["demo.schema.v1"]
    assert payload["rule_metrics"]["demo.schema.v1"]["dimension"] == "completeness"


def test_non_rejecting_warning_records_metrics_without_invalidating_rows():
    frame = pl.DataFrame({"value": [1, 2]})
    warning = RowRule(
        rule_id="demo.warning.v1",
        dimension=DamaDimension.ACCURACY,
        severity=RuleSeverity.WARNING,
        description="Example advisory signal",
        failure_expression=lambda _: pl.col("value") == 2,
        reject_rows=False,
    )

    result = evaluate_rules(frame, row_rules=(warning,))

    assert result.invalid_row_count == 0
    assert result.rule_metrics["demo.warning.v1"]["failed_count"] == 1
    assert result.issues[0].severity == "WARNING"


def test_clean_sensor_and_mes_fixtures_pass_contracts():
    for table_name in ("sensor_telemetry", "mes_lims"):
        result = validate_table(_canonical_fixture(table_name), table_name)
        assert not result.hard_failures
        assert result.invalid_row_count == 0


def test_sensor_range_boundaries_and_outside_values_are_exact():
    frame = pl.concat([_canonical_fixture("sensor_telemetry").head(1)] * 4)
    frame = frame.with_columns(
        pl.Series("timestamp", [
            datetime(2026, 1, 1, 0, index, tzinfo=timezone.utc)
            for index in range(4)
        ]),
        pl.Series("uht_temp", [135.0, 145.0, 134.9, 145.1], dtype=pl.Float32),
    )

    result = validate_table(frame, "sensor_telemetry")

    issue = next(issue for issue in result.issues if issue.rule_id == "sensor.uht_temp.range.v1")
    assert issue.affected_row_indices == (2, 3)
    assert result.invalid_row_mask == (False, False, True, True)


def test_sensor_null_ratio_allows_exactly_one_percent_and_rejects_above_it():
    row = _canonical_fixture("sensor_telemetry").head(1)
    frame = pl.concat([row] * 100).with_columns(
        pl.Series(
            "timestamp",
            [
                datetime(2026, 1, 1, tzinfo=timezone.utc) + timedelta(seconds=index)
                for index in range(100)
            ],
        ),
        pl.Series(
            "homo_press_stage1",
            [None] + [200.0] * 99,
            dtype=pl.Float32,
        ),
    )
    exact = validate_table(frame, "sensor_telemetry")
    above = validate_table(
        frame.with_columns(
            pl.when(pl.int_range(pl.len()) < 2)
            .then(None)
            .otherwise(pl.col("homo_press_stage1"))
            .alias("homo_press_stage1")
        ),
        "sensor_telemetry",
    )

    rule_id = "sensor.homo_press_stage1.null_ratio.v1"
    assert exact.rule_metrics[rule_id]["failed_count"] == 0
    assert above.rule_metrics[rule_id]["failed_count"] == 2


def test_mes_contract_identifies_category_time_percentage_and_duplicate_errors():
    base_row = _canonical_fixture("mes_lims").head(1)
    base = pl.concat([base_row, base_row])
    broken = base.with_columns(
        pl.lit(base["batch_id"][0]).alias("batch_id"),
        pl.when(pl.int_range(pl.len()) == 0)
        .then(pl.lit("UNKNOWN_SKU"))
        .otherwise(pl.col("sku_id"))
        .alias("sku_id"),
        pl.when(pl.int_range(pl.len()) == 0)
        .then(pl.col("production_start") - pl.duration(hours=1))
        .otherwise(pl.col("production_end"))
        .alias("production_end"),
        pl.when(pl.int_range(pl.len()) == 1)
        .then(pl.lit(101.0))
        .otherwise(pl.col("yield_recovery_pct"))
        .cast(pl.Float32)
        .alias("yield_recovery_pct"),
    )

    result = validate_table(broken, "mes_lims")
    failed_ids = {issue.rule_id for issue in result.issues}

    assert "mes.batch_id.unique.v1" in failed_ids
    assert "mes.sku_id.category.v1" in failed_ids
    assert "mes.production_time.order.v1" in failed_ids
    assert "mes.yield_recovery_pct.range.v1" in failed_ids


def test_missing_required_column_and_wrong_type_are_hard_failures():
    sensor = _canonical_fixture("sensor_telemetry")
    missing = validate_table(sensor.drop("uht_temp"), "sensor_telemetry")
    wrong_type = validate_table(
        sensor.with_columns(pl.col("uht_temp").cast(pl.String)),
        "sensor_telemetry",
    )

    assert "sensor_telemetry.required_columns.v1" in missing.hard_failures
    assert "sensor_telemetry.canonical_types.v1" in wrong_type.hard_failures


def test_clean_external_fixtures_pass_row_contracts():
    for table_name in ("weather", "market_prices", "market_indices", "food_recalls"):
        result = validate_table(_canonical_fixture(table_name), table_name)
        assert not result.hard_failures
        assert result.invalid_row_count == 0


def test_weather_contract_finds_ranges_identity_and_thi_errors_as_rows():
    row = _canonical_fixture("weather").head(1)
    broken = pl.concat([row, row]).with_columns(
        pl.when(pl.int_range(pl.len()) == 0)
        .then(pl.lit("UNKNOWN_FARM"))
        .otherwise(pl.col("farm_id"))
        .alias("farm_id"),
        pl.when(pl.int_range(pl.len()) == 0)
        .then(pl.lit(50.0))
        .otherwise(pl.col("temperature_c"))
        .cast(pl.Float32)
        .alias("temperature_c"),
        pl.when(pl.int_range(pl.len()) == 1)
        .then(pl.lit(101.0))
        .otherwise(pl.col("relative_humidity_pct"))
        .cast(pl.Float32)
        .alias("relative_humidity_pct"),
        pl.when(pl.int_range(pl.len()) == 1)
        .then(pl.lit(-1.0))
        .otherwise(pl.col("precipitation_mm"))
        .cast(pl.Float32)
        .alias("precipitation_mm"),
        pl.lit(None).cast(pl.Float32).alias("thi_index"),
    )

    result = validate_table(broken, "weather")
    failed = {issue.rule_id for issue in result.issues}

    assert "weather.farm_id.category.v1" in failed
    assert "weather.temperature_c.range.v1" in failed
    assert "weather.relative_humidity_pct.range.v1" in failed
    assert "weather.precipitation_mm.non_negative.v1" in failed
    assert "weather.thi_index.required_for_inputs.v1" in failed
    assert result.rule_metrics["weather.daily_coverage.warning.v1"]["passed"] is False


def test_weather_required_and_unique_rules_have_negative_coverage():
    row = _canonical_fixture("weather").head(1)
    duplicate = validate_table(pl.concat([row, row]), "weather")
    missing_values = validate_table(
        pl.concat([row, row]).with_columns(
            pl.when(pl.int_range(pl.len()) == 0)
            .then(None)
            .otherwise(pl.col("farm_id"))
            .alias("farm_id"),
            pl.when(pl.int_range(pl.len()) == 1)
            .then(None)
            .otherwise(pl.col("observed_at"))
            .alias("observed_at"),
        ),
        "weather",
    )

    duplicate_ids = {issue.rule_id for issue in duplicate.issues}
    missing_ids = {issue.rule_id for issue in missing_values.issues}
    assert "weather.farm_id_observed_at.unique.v1" in duplicate_ids
    assert "weather.farm_id.required.v1" in missing_ids
    assert "weather.observed_at.required.v1" in missing_ids


def test_gdt_contract_finds_product_price_key_and_unit_errors():
    row = _canonical_fixture("market_prices").head(1)
    broken = pl.concat([row, row]).with_columns(
        pl.when(pl.int_range(pl.len()) == 0)
        .then(pl.lit("UNKNOWN"))
        .otherwise(pl.col("product"))
        .alias("product"),
        pl.when(pl.int_range(pl.len()) == 0)
        .then(pl.lit(-1.0))
        .otherwise(pl.col("price"))
        .cast(pl.Float32)
        .alias("price"),
        pl.when(pl.int_range(pl.len()) == 1)
        .then(pl.lit("lb"))
        .otherwise(pl.col("unit"))
        .alias("unit"),
    )
    duplicate = pl.concat([row, row])

    failed = {issue.rule_id for issue in validate_table(broken, "market_prices").issues}
    duplicate_failed = {
        issue.rule_id for issue in validate_table(duplicate, "market_prices").issues
    }

    assert "gdt.product.category.v1" in failed
    assert "gdt.price.positive.v1" in failed
    assert "gdt.unit.constant.v1" in failed
    assert "gdt.source_product_observed_at_contract.unique.v1" in duplicate_failed


def test_gdt_required_and_constant_rules_have_negative_coverage():
    row = _canonical_fixture("market_prices").head(1)
    frames = [row] * 7
    broken = pl.concat(frames).with_columns(
        pl.when(pl.int_range(pl.len()) == 0)
        .then(pl.lit("OTHER"))
        .when(pl.int_range(pl.len()) == 1)
        .then(None)
        .otherwise(pl.col("source"))
        .alias("source"),
        pl.when(pl.int_range(pl.len()) == 2)
        .then(None)
        .otherwise(pl.col("product"))
        .alias("product"),
        pl.when(pl.int_range(pl.len()) == 3)
        .then(None)
        .otherwise(pl.col("observed_at"))
        .alias("observed_at"),
        pl.when(pl.int_range(pl.len()) == 4)
        .then(None)
        .otherwise(pl.col("price"))
        .cast(pl.Float32)
        .alias("price"),
        pl.when(pl.int_range(pl.len()) == 5)
        .then(pl.lit("EUR"))
        .when(pl.int_range(pl.len()) == 6)
        .then(None)
        .otherwise(pl.col("currency"))
        .alias("currency"),
    )
    failed = {issue.rule_id for issue in validate_table(broken, "market_prices").issues}

    assert {
        "gdt.source.required.v1",
        "gdt.product.required.v1",
        "gdt.observed_at.required.v1",
        "gdt.price.required.v1",
        "gdt.currency.required.v1",
        "gdt.source.constant.v1",
        "gdt.currency.constant.v1",
    }.issubset(failed)


def test_fao_contract_finds_non_positive_duplicate_and_month_gap():
    row = _canonical_fixture("market_indices").head(1)
    duplicate = pl.concat([row, row]).with_columns(
        pl.when(pl.int_range(pl.len()) == 0)
        .then(pl.lit(-1.0))
        .otherwise(pl.col("index_value"))
        .cast(pl.Float32)
        .alias("index_value")
    )
    march = row.with_columns(pl.lit(datetime(2025, 3, 1).date()).alias("period_start"))
    gap = pl.concat([row, march])

    failed = {issue.rule_id for issue in validate_table(duplicate, "market_indices").issues}
    gap_result = validate_table(gap, "market_indices")

    assert "fao.index_value.positive.v1" in failed
    assert "fao.source_index_name_period_start.unique.v1" in failed
    assert gap_result.invalid_row_count == 0
    assert gap_result.rule_metrics["fao.month_continuity.warning.v1"]["passed"] is False


def test_fao_required_and_source_rules_have_negative_coverage():
    row = _canonical_fixture("market_indices").head(1)
    broken = pl.concat([row] * 5).with_columns(
        pl.when(pl.int_range(pl.len()) == 0)
        .then(pl.lit("OTHER"))
        .when(pl.int_range(pl.len()) == 1)
        .then(None)
        .otherwise(pl.col("source"))
        .alias("source"),
        pl.when(pl.int_range(pl.len()) == 2)
        .then(None)
        .otherwise(pl.col("index_name"))
        .alias("index_name"),
        pl.when(pl.int_range(pl.len()) == 3)
        .then(None)
        .otherwise(pl.col("period_start"))
        .alias("period_start"),
        pl.when(pl.int_range(pl.len()) == 4)
        .then(None)
        .otherwise(pl.col("index_value"))
        .cast(pl.Float32)
        .alias("index_value"),
    )
    failed = {issue.rule_id for issue in validate_table(broken, "market_indices").issues}

    assert {
        "fao.source.required.v1",
        "fao.index_name.required.v1",
        "fao.period_start.required.v1",
        "fao.index_value.required.v1",
        "fao.source.constant.v1",
    }.issubset(failed)


def test_fda_contract_finds_classification_identity_and_future_date():
    row = _canonical_fixture("food_recalls").head(1)
    broken = pl.concat([row, row]).with_columns(
        pl.when(pl.int_range(pl.len()) == 0)
        .then(pl.lit("Class IV"))
        .otherwise(pl.col("classification"))
        .alias("classification"),
        pl.when(pl.int_range(pl.len()) == 1)
        .then(pl.col("ingested_at") + pl.duration(days=1))
        .otherwise(pl.col("published_at"))
        .alias("published_at"),
    )

    failed = {issue.rule_id for issue in validate_table(broken, "food_recalls").issues}

    assert "fda.recall_id.unique.v1" in failed
    assert "fda.classification.category.v1" in failed
    assert "fda.published_at.not_after_crawl.v1" in failed


def test_fda_required_and_source_rules_have_negative_coverage():
    row = _canonical_fixture("food_recalls").head(1)
    broken = pl.concat([row] * 5).with_columns(
        pl.when(pl.int_range(pl.len()) == 0)
        .then(pl.lit("OTHER"))
        .when(pl.int_range(pl.len()) == 1)
        .then(None)
        .otherwise(pl.col("source"))
        .alias("source"),
        pl.when(pl.int_range(pl.len()) == 2)
        .then(None)
        .otherwise(pl.col("recall_id"))
        .alias("recall_id"),
        pl.when(pl.int_range(pl.len()) == 3)
        .then(None)
        .otherwise(pl.col("published_at"))
        .alias("published_at"),
        pl.when(pl.int_range(pl.len()) == 4)
        .then(None)
        .otherwise(pl.col("classification"))
        .alias("classification"),
    )
    failed = {issue.rule_id for issue in validate_table(broken, "food_recalls").issues}

    assert {
        "fda.source.constant.v1",
        "fda.recall_id.required.v1",
        "fda.published_at.required.v1",
        "fda.classification.required.v1",
    }.issubset(failed)


def test_usda_has_no_silver_quality_contract():
    with pytest.raises(ValueError, match="Unsupported Silver contract"):
        validate_table(pl.DataFrame(), "market_reports")

