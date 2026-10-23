"""Versioned operational quality contracts for canonical Silver tables."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Callable

import polars as pl

from data_governance.config import CONTRACT_VERSION
from data_governance.models import QualityResult
from data_governance.validator import (
    DamaDimension,
    RowRule,
    RuleSeverity,
    TableRule,
    TableRuleOutcome,
    evaluate_rules,
)
from lakehouse_storage.schemas import (
    FARM_LOCATIONS,
    UNSUPPORTED_SILVER_TABLES,
    normalize_table_name,
)


@dataclass(frozen=True)
class TableContract:
    required_columns: tuple[str, ...]
    expected_types: dict[str, pl.DataType]
    row_rules: Callable[[pl.DataFrame], tuple[RowRule, ...]]
    table_rules: Callable[[pl.DataFrame], tuple[TableRule, ...]] = lambda _: ()


@dataclass(frozen=True)
class GxExpectationSpec:
    rule_id: str
    expectation_type: str
    kwargs: dict[str, object]


def _required_rule(table_name: str, columns: tuple[str, ...]) -> TableRule:
    def evaluate(df: pl.DataFrame) -> TableRuleOutcome:
        missing = sorted(set(columns) - set(df.columns))
        return TableRuleOutcome(
            passed=not missing,
            observed={"required_columns": list(columns), "missing_columns": missing},
            message=f"Missing required columns: {missing}" if missing else None,
        )

    return TableRule(
        rule_id=f"{table_name}.required_columns.v1",
        dimension=DamaDimension.COMPLETENESS,
        severity=RuleSeverity.ERROR,
        description="All required canonical columns must exist",
        evaluator=evaluate,
        hard_failure=True,
    )


def _type_rule(table_name: str, expected_types: dict[str, pl.DataType]) -> TableRule:
    def evaluate(df: pl.DataFrame) -> TableRuleOutcome:
        mismatches = {
            column: {"expected": str(expected), "actual": str(df.schema[column])}
            for column, expected in expected_types.items()
            if column in df.columns and df.schema[column] != expected
        }
        return TableRuleOutcome(
            passed=not mismatches,
            observed={"type_mismatches": mismatches},
            message=f"Canonical type mismatches: {mismatches}" if mismatches else None,
        )

    return TableRule(
        rule_id=f"{table_name}.canonical_types.v1",
        dimension=DamaDimension.CONSISTENCY,
        severity=RuleSeverity.ERROR,
        description="Canonical columns must use their v1 data types",
        evaluator=evaluate,
        hard_failure=True,
    )


def _non_empty_rule(table_name: str) -> TableRule:
    return TableRule(
        rule_id=f"{table_name}.non_empty.v1",
        dimension=DamaDimension.COMPLETENESS,
        severity=RuleSeverity.ERROR,
        description="A governance batch must contain at least one row",
        evaluator=lambda df: TableRuleOutcome(
            passed=df.height > 0,
            observed={"row_count": df.height},
            message="Input dataframe is empty" if df.is_empty() else None,
        ),
        hard_failure=True,
    )


def _required_value_rule(table_name: str, column: str) -> RowRule:
    return RowRule(
        rule_id=f"{table_name}.{column}.required.v1",
        dimension=DamaDimension.COMPLETENESS,
        severity=RuleSeverity.ERROR,
        description=f"{column} must not be null",
        failure_expression=lambda _, name=column: pl.col(name).is_null(),
    )


def _range_rule(
    table_name: str,
    column: str,
    minimum: float,
    maximum: float,
) -> RowRule:
    return RowRule(
        rule_id=f"{table_name}.{column}.range.v1",
        dimension=DamaDimension.VALIDITY,
        severity=RuleSeverity.ERROR,
        description=f"{column} must be between {minimum} and {maximum}",
        failure_expression=lambda _, name=column, low=minimum, high=maximum: (
            pl.col(name).is_not_null()
            & ~pl.col(name).is_between(low, high, closed="both")
        ),
    )


def _unique_rule(table_name: str, columns: tuple[str, ...]) -> RowRule:
    identity = "_".join(columns)
    return RowRule(
        rule_id=f"{table_name}.{identity}.unique.v1",
        dimension=DamaDimension.UNIQUENESS,
        severity=RuleSeverity.ERROR,
        description=f"Business key {columns} must be unique",
        failure_expression=lambda _, names=columns: pl.struct(list(names)).is_duplicated(),
    )


def _category_rule(
    table_name: str,
    column: str,
    accepted: tuple[str, ...],
    *,
    allow_null: bool = False,
) -> RowRule:
    invalid = ~pl.col(column).is_in(list(accepted))
    if allow_null:
        invalid = pl.col(column).is_not_null() & invalid
    return RowRule(
        rule_id=f"{table_name}.{column}.category.v1",
        dimension=DamaDimension.VALIDITY,
        severity=RuleSeverity.ERROR,
        description=f"{column} must be one of {accepted}",
        failure_expression=lambda _, expression=invalid: expression,
    )


def _constant_rule(table_name: str, column: str, expected: str) -> RowRule:
    return RowRule(
        rule_id=f"{table_name}.{column}.constant.v1",
        dimension=DamaDimension.CONSISTENCY,
        severity=RuleSeverity.ERROR,
        description=f"{column} must equal {expected}",
        failure_expression=lambda _, name=column, value=expected: pl.col(name) != value,
    )


SENSOR_FLOAT_COLUMNS = (
    "preheat_temp",
    "uht_temp",
    "homo_press_stage1",
    "homo_press_stage2",
    "flow_rate",
    "conductivity",
    "power_kw",
)

SENSOR_IMPORTANT_NULL_COLUMNS = (
    "preheat_temp",
    "homo_press_stage1",
    "homo_press_stage2",
    "flow_rate",
    "conductivity",
    "power_kw",
)


def _sensor_rules(df: pl.DataFrame) -> tuple[RowRule, ...]:
    rules: list[RowRule] = [
        *(
            _required_value_rule("sensor", column)
            for column in ("timestamp", "line_id", "uht_temp")
        ),
        _unique_rule("sensor", ("line_id", "timestamp")),
        _range_rule("sensor", "uht_temp", 135.0, 145.0),
        _range_rule("sensor", "homo_press_stage1", 150.0, 250.0),
        _range_rule("sensor", "homo_press_stage2", 20.0, 60.0),
        RowRule(
            rule_id="sensor.flow_rate.positive.v1",
            dimension=DamaDimension.VALIDITY,
            severity=RuleSeverity.ERROR,
            description="flow_rate must be positive when present",
            failure_expression=lambda _: (
                pl.col("flow_rate").is_not_null() & (pl.col("flow_rate") <= 0)
            ),
        ),
    ]
    for column in SENSOR_IMPORTANT_NULL_COLUMNS:
        if column in df.columns:
            rules.append(
                RowRule(
                    rule_id=f"sensor.{column}.null_ratio.v1",
                    dimension=DamaDimension.COMPLETENESS,
                    severity=RuleSeverity.ERROR,
                    description=f"{column} null ratio must not exceed 1%",
                    failure_expression=lambda _, name=column: pl.when(
                        pl.col(name).is_null().sum() / pl.len() > 0.01
                    )
                    .then(pl.col(name).is_null())
                    .otherwise(False),
                )
            )
    return tuple(rules)


def _mes_rules(_: pl.DataFrame) -> tuple[RowRule, ...]:
    rules: list[RowRule] = [
        *(
            _required_value_rule("mes", column)
            for column in ("batch_id", "line_id", "sku_id", "batch_defect_flag")
        ),
        _unique_rule("mes", ("batch_id",)),
        RowRule(
            rule_id="mes.production_time.order.v1",
            dimension=DamaDimension.CONSISTENCY,
            severity=RuleSeverity.ERROR,
            description="production_end must not precede production_start",
            failure_expression=lambda _: (
                pl.col("production_start").is_not_null()
                & pl.col("production_end").is_not_null()
                & (pl.col("production_end") < pl.col("production_start"))
            ),
        ),
        RowRule(
            rule_id="mes.sku_id.category.v1",
            dimension=DamaDimension.VALIDITY,
            severity=RuleSeverity.ERROR,
            description="sku_id must be a supported UHT SKU",
            failure_expression=lambda _: ~pl.col("sku_id").is_in(
                ["UHT_PURE", "UHT_SWEETENED", "UHT_LOWFAT"]
            ),
        ),
        RowRule(
            rule_id="mes.microbiology_status.category.v1",
            dimension=DamaDimension.VALIDITY,
            severity=RuleSeverity.ERROR,
            description="microbiology_status must be PASSED, FAILED, or PENDING",
            failure_expression=lambda _: (
                pl.col("microbiology_status").is_not_null()
                & ~pl.col("microbiology_status").is_in(
                    ["PASSED", "FAILED", "PENDING"]
                )
            ),
        ),
        RowRule(
            rule_id="mes.batch_defect_flag.category.v1",
            dimension=DamaDimension.VALIDITY,
            severity=RuleSeverity.ERROR,
            description="batch_defect_flag must be 0 or 1",
            failure_expression=lambda _: ~pl.col("batch_defect_flag").is_in([0, 1]),
        ),
    ]
    rules.extend(
        _range_rule("mes", column, 0.0, 100.0)
        for column in ("fat_in", "protein_in", "yield_recovery_pct")
    )
    return tuple(rules)


def _weather_rules(_: pl.DataFrame) -> tuple[RowRule, ...]:
    return (
        _required_value_rule("weather", "farm_id"),
        _required_value_rule("weather", "observed_at"),
        _unique_rule("weather", ("farm_id", "observed_at")),
        _category_rule("weather", "farm_id", tuple(FARM_LOCATIONS)),
        _range_rule("weather", "temperature_c", -10.0, 45.0),
        _range_rule("weather", "relative_humidity_pct", 0.0, 100.0),
        RowRule(
            rule_id="weather.precipitation_mm.non_negative.v1",
            dimension=DamaDimension.VALIDITY,
            severity=RuleSeverity.ERROR,
            description="precipitation_mm must be non-negative when present",
            failure_expression=lambda _: (
                pl.col("precipitation_mm").is_not_null()
                & (pl.col("precipitation_mm") < 0)
            ),
        ),
        RowRule(
            rule_id="weather.thi_index.required_for_inputs.v1",
            dimension=DamaDimension.COMPLETENESS,
            severity=RuleSeverity.ERROR,
            description="thi_index is required when temperature and humidity exist",
            failure_expression=lambda _: (
                pl.col("temperature_c").is_not_null()
                & pl.col("relative_humidity_pct").is_not_null()
                & pl.col("thi_index").is_null()
            ),
        ),
    )


def _weather_table_rules(_: pl.DataFrame) -> tuple[TableRule, ...]:
    def daily_coverage(df: pl.DataFrame) -> TableRuleOutcome:
        counts = (
            df.group_by("farm_id", "ingestion_date")
            .len(name="observed_count")
            .filter(pl.col("observed_count") != 24)
        )
        incomplete = counts.to_dicts()
        return TableRuleOutcome(
            passed=not incomplete,
            observed={"expected_per_day": 24, "incomplete_groups": incomplete},
            message=(
                "One or more farm/day groups are incomplete; boundary days are advisory"
                if incomplete
                else None
            ),
        )

    return (
        TableRule(
            rule_id="weather.daily_coverage.warning.v1",
            dimension=DamaDimension.COMPLETENESS,
            severity=RuleSeverity.WARNING,
            description="A complete UTC farm/day normally contains 24 observations",
            evaluator=daily_coverage,
        ),
    )


def _gdt_rules(_: pl.DataFrame) -> tuple[RowRule, ...]:
    return (
        *(
            _required_value_rule("gdt", column)
            for column in ("source", "product", "observed_at", "price", "currency")
        ),
        _unique_rule("gdt", ("source", "product", "observed_at", "contract")),
        _category_rule("gdt", "product", ("WMP", "SMP", "AMF")),
        RowRule(
            rule_id="gdt.price.positive.v1",
            dimension=DamaDimension.VALIDITY,
            severity=RuleSeverity.ERROR,
            description="GDT price must be positive",
            failure_expression=lambda _: pl.col("price").is_not_null()
            & (pl.col("price") <= 0),
        ),
        _constant_rule("gdt", "source", "GDT"),
        _constant_rule("gdt", "currency", "USD"),
        _constant_rule("gdt", "unit", "metric_ton"),
    )


def _fao_rules(_: pl.DataFrame) -> tuple[RowRule, ...]:
    return (
        *(
            _required_value_rule("fao", column)
            for column in ("source", "index_name", "period_start", "index_value")
        ),
        _unique_rule("fao", ("source", "index_name", "period_start")),
        RowRule(
            rule_id="fao.index_value.positive.v1",
            dimension=DamaDimension.VALIDITY,
            severity=RuleSeverity.ERROR,
            description="FAO index_value must be positive",
            failure_expression=lambda _: pl.col("index_value").is_not_null()
            & (pl.col("index_value") <= 0),
        ),
        _constant_rule("fao", "source", "FAO"),
    )


def _fao_table_rules(_: pl.DataFrame) -> tuple[TableRule, ...]:
    def month_continuity(df: pl.DataFrame) -> TableRuleOutcome:
        gaps: list[dict[str, str]] = []
        for group in df.group_by("index_name").agg(
            pl.col("period_start").drop_nulls().unique().sort()
        ).to_dicts():
            dates = group["period_start"]
            for previous, current in zip(dates, dates[1:]):
                month_gap = (current.year - previous.year) * 12 + current.month - previous.month
                if month_gap > 1:
                    gaps.append(
                        {
                            "index_name": group["index_name"],
                            "previous": previous.isoformat(),
                            "current": current.isoformat(),
                        }
                    )
        return TableRuleOutcome(
            passed=not gaps,
            observed={"month_gaps": gaps},
            message="FAO series contains one or more month gaps" if gaps else None,
        )

    return (
        TableRule(
            rule_id="fao.month_continuity.warning.v1",
            dimension=DamaDimension.COMPLETENESS,
            severity=RuleSeverity.WARNING,
            description="FAO month gaps are advisory unless continuity is required",
            evaluator=month_continuity,
        ),
    )


def _fda_rules(_: pl.DataFrame) -> tuple[RowRule, ...]:
    return (
        *(
            _required_value_rule("fda", column)
            for column in ("recall_id", "published_at", "classification")
        ),
        _unique_rule("fda", ("recall_id",)),
        _category_rule(
            "fda",
            "classification",
            ("Class I", "Class II", "Class III"),
        ),
        _constant_rule("fda", "source", "FDA"),
        RowRule(
            rule_id="fda.published_at.not_after_crawl.v1",
            dimension=DamaDimension.TIMELINESS,
            severity=RuleSeverity.ERROR,
            description="published_at must not be later than ingested_at",
            failure_expression=lambda _: (
                pl.col("published_at").is_not_null()
                & pl.col("ingested_at").is_not_null()
                & (pl.col("published_at") > pl.col("ingested_at"))
            ),
        ),
    )


_SENSOR_CONTRACT = TableContract(
    required_columns=("timestamp", "line_id", "uht_temp"),
    expected_types={
        "timestamp": pl.Datetime("us", "UTC"),
        "line_id": pl.String,
        "batch_id": pl.String,
        **{column: pl.Float32 for column in SENSOR_FLOAT_COLUMNS},
    },
    row_rules=_sensor_rules,
)

_MES_CONTRACT = TableContract(
    required_columns=("batch_id", "line_id", "sku_id", "batch_defect_flag"),
    expected_types={
        "batch_id": pl.String,
        "line_id": pl.String,
        "production_start": pl.Datetime("us", "UTC"),
        "production_end": pl.Datetime("us", "UTC"),
        "sku_id": pl.String,
        "fat_in": pl.Float32,
        "protein_in": pl.Float32,
        "acidity_sh": pl.Float32,
        "yield_recovery_pct": pl.Float32,
        "microbiology_status": pl.String,
        "batch_defect_flag": pl.Int8,
    },
    row_rules=_mes_rules,
)

_WEATHER_CONTRACT = TableContract(
    required_columns=("farm_id", "observed_at"),
    expected_types={
        "farm_id": pl.String,
        "observed_at": pl.Datetime("us", "UTC"),
        "temperature_c": pl.Float32,
        "relative_humidity_pct": pl.Float32,
        "precipitation_mm": pl.Float32,
        "thi_index": pl.Float32,
        "is_heat_stress": pl.Boolean,
        "ingested_at": pl.Datetime("us", "UTC"),
    },
    row_rules=_weather_rules,
    table_rules=_weather_table_rules,
)

_GDT_CONTRACT = TableContract(
    required_columns=("source", "product", "observed_at", "currency", "ingested_at"),
    expected_types={
        "source": pl.String,
        "product": pl.String,
        "contract": pl.String,
        "observed_at": pl.Datetime("us", "UTC"),
        "price": pl.Float32,
        "currency": pl.String,
        "unit": pl.String,
        "ingested_at": pl.Datetime("us", "UTC"),
    },
    row_rules=_gdt_rules,
)

_FAO_CONTRACT = TableContract(
    required_columns=("source", "index_name", "period_start", "ingested_at"),
    expected_types={
        "source": pl.String,
        "index_name": pl.String,
        "period_start": pl.Date,
        "index_value": pl.Float32,
        "base_period": pl.String,
        "ingested_at": pl.Datetime("us", "UTC"),
    },
    row_rules=_fao_rules,
    table_rules=_fao_table_rules,
)

_FDA_CONTRACT = TableContract(
    required_columns=("source", "recall_id", "published_at", "classification", "ingested_at"),
    expected_types={
        "source": pl.String,
        "recall_id": pl.String,
        "published_at": pl.Datetime("us", "UTC"),
        "product": pl.String,
        "reason": pl.String,
        "status": pl.String,
        "classification": pl.String,
        "source_url": pl.String,
        "ingested_at": pl.Datetime("us", "UTC"),
    },
    row_rules=_fda_rules,
)

CONTRACTS: dict[str, TableContract] = {
    "sensor_telemetry": _SENSOR_CONTRACT,
    "mes_lims": _MES_CONTRACT,
    "weather": _WEATHER_CONTRACT,
    "market_prices": _GDT_CONTRACT,
    "market_indices": _FAO_CONTRACT,
    "food_recalls": _FDA_CONTRACT,
}


_GX_BUSINESS_SPECS: dict[str, tuple[GxExpectationSpec, ...]] = {
    "sensor_telemetry": (
        GxExpectationSpec("sensor.line_id_timestamp.unique.v1", "ExpectCompoundColumnsToBeUnique", {"column_list": ["line_id", "timestamp"]}),
        GxExpectationSpec("sensor.uht_temp.range.v1", "ExpectColumnValuesToBeBetween", {"column": "uht_temp", "min_value": 135.0, "max_value": 145.0}),
        GxExpectationSpec("sensor.homo_press_stage1.range.v1", "ExpectColumnValuesToBeBetween", {"column": "homo_press_stage1", "min_value": 150.0, "max_value": 250.0}),
        GxExpectationSpec("sensor.homo_press_stage2.range.v1", "ExpectColumnValuesToBeBetween", {"column": "homo_press_stage2", "min_value": 20.0, "max_value": 60.0}),
        GxExpectationSpec("sensor.flow_rate.positive.v1", "ExpectColumnValuesToBeBetween", {"column": "flow_rate", "min_value": 0.0, "strict_min": True}),
    ),
    "mes_lims": (
        GxExpectationSpec("mes.batch_id.unique.v1", "ExpectColumnValuesToBeUnique", {"column": "batch_id"}),
        GxExpectationSpec("mes.sku_id.category.v1", "ExpectColumnValuesToBeInSet", {"column": "sku_id", "value_set": ["UHT_PURE", "UHT_SWEETENED", "UHT_LOWFAT"]}),
        GxExpectationSpec("mes.microbiology_status.category.v1", "ExpectColumnValuesToBeInSet", {"column": "microbiology_status", "value_set": ["PASSED", "FAILED", "PENDING"]}),
        GxExpectationSpec("mes.batch_defect_flag.category.v1", "ExpectColumnValuesToBeInSet", {"column": "batch_defect_flag", "value_set": [0, 1]}),
        GxExpectationSpec("mes.yield_recovery_pct.range.v1", "ExpectColumnValuesToBeBetween", {"column": "yield_recovery_pct", "min_value": 0.0, "max_value": 100.0}),
        GxExpectationSpec("mes.production_time.order.v1", "ExpectColumnPairValuesAToBeGreaterThanB", {"column_A": "production_end", "column_B": "production_start", "or_equal": True, "ignore_row_if": "either_value_is_missing"}),
    ),
    "weather": (
        GxExpectationSpec("weather.farm_id_observed_at.unique.v1", "ExpectCompoundColumnsToBeUnique", {"column_list": ["farm_id", "observed_at"]}),
        GxExpectationSpec("weather.farm_id.category.v1", "ExpectColumnValuesToBeInSet", {"column": "farm_id", "value_set": list(FARM_LOCATIONS)}),
        GxExpectationSpec("weather.temperature_c.range.v1", "ExpectColumnValuesToBeBetween", {"column": "temperature_c", "min_value": -10.0, "max_value": 45.0}),
        GxExpectationSpec("weather.relative_humidity_pct.range.v1", "ExpectColumnValuesToBeBetween", {"column": "relative_humidity_pct", "min_value": 0.0, "max_value": 100.0}),
        GxExpectationSpec("weather.precipitation_mm.non_negative.v1", "ExpectColumnValuesToBeBetween", {"column": "precipitation_mm", "min_value": 0.0}),
    ),
    "market_prices": (
        GxExpectationSpec("gdt.source_product_observed_at_contract.unique.v1", "ExpectCompoundColumnsToBeUnique", {"column_list": ["source", "product", "observed_at", "contract"]}),
        GxExpectationSpec("gdt.product.category.v1", "ExpectColumnValuesToBeInSet", {"column": "product", "value_set": ["WMP", "SMP", "AMF"]}),
        GxExpectationSpec("gdt.price.positive.v1", "ExpectColumnValuesToBeBetween", {"column": "price", "min_value": 0.0, "strict_min": True}),
        GxExpectationSpec("gdt.currency.constant.v1", "ExpectColumnValuesToBeInSet", {"column": "currency", "value_set": ["USD"]}),
        GxExpectationSpec("gdt.unit.constant.v1", "ExpectColumnValuesToBeInSet", {"column": "unit", "value_set": ["metric_ton"]}),
    ),
    "market_indices": (
        GxExpectationSpec("fao.source_index_name_period_start.unique.v1", "ExpectCompoundColumnsToBeUnique", {"column_list": ["source", "index_name", "period_start"]}),
        GxExpectationSpec("fao.index_value.positive.v1", "ExpectColumnValuesToBeBetween", {"column": "index_value", "min_value": 0.0, "strict_min": True}),
        GxExpectationSpec("fao.source.constant.v1", "ExpectColumnValuesToBeInSet", {"column": "source", "value_set": ["FAO"]}),
    ),
    "food_recalls": (
        GxExpectationSpec("fda.recall_id.unique.v1", "ExpectColumnValuesToBeUnique", {"column": "recall_id"}),
        GxExpectationSpec("fda.classification.category.v1", "ExpectColumnValuesToBeInSet", {"column": "classification", "value_set": ["Class I", "Class II", "Class III"]}),
        GxExpectationSpec("fda.source.constant.v1", "ExpectColumnValuesToBeInSet", {"column": "source", "value_set": ["FDA"]}),
        GxExpectationSpec("fda.published_at.not_after_crawl.v1", "ExpectColumnPairValuesAToBeGreaterThanB", {"column_A": "ingested_at", "column_B": "published_at", "or_equal": True, "ignore_row_if": "either_value_is_missing"}),
    ),
}


def gx_expectation_specs(table_name: str) -> tuple[GxExpectationSpec, ...]:
    """Build GX mirror specs from the operational contract registry."""
    normalized_name = normalize_table_name(table_name)
    contract = CONTRACTS.get(normalized_name)
    if contract is None:
        raise ValueError(f"Unknown governance contract: {normalized_name}")
    required = tuple(
        GxExpectationSpec(
            rule_id=f"{normalized_name}.{column}.required.v1",
            expectation_type="ExpectColumnValuesToNotBeNull",
            kwargs={"column": column},
        )
        for column in contract.required_columns
    )
    non_empty = GxExpectationSpec(
        rule_id=f"{normalized_name}.non_empty.v1",
        expectation_type="ExpectTableRowCountToBeBetween",
        kwargs={"min_value": 1},
    )
    return (non_empty, *required, *_GX_BUSINESS_SPECS[normalized_name])


def _merge_results(first: QualityResult, second: QualityResult) -> QualityResult:
    if first.total_row_count != second.total_row_count:
        raise ValueError("Cannot merge quality results with different row counts")
    return QualityResult(
        total_row_count=first.total_row_count,
        invalid_row_mask=tuple(
            left or right
            for left, right in zip(
                first.invalid_row_mask,
                second.invalid_row_mask,
                strict=True,
            )
        ),
        rule_metrics={**first.rule_metrics, **second.rule_metrics},
        hard_failures=first.hard_failures + second.hard_failures,
        issues=first.issues + second.issues,
    )


def validate_table(
    df: pl.DataFrame,
    table_name: str,
    contract_version: str = CONTRACT_VERSION,
) -> QualityResult:
    """Validate a canonical dataframe against its versioned table contract."""
    if contract_version != CONTRACT_VERSION:
        raise ValueError(f"Unsupported contract version: {contract_version}")
    normalized_name = normalize_table_name(table_name)
    if normalized_name in UNSUPPORTED_SILVER_TABLES:
        raise ValueError(f"Unsupported Silver contract: {normalized_name}")
    contract = CONTRACTS.get(normalized_name)
    if contract is None:
        raise ValueError(f"Unknown governance contract: {normalized_name}")

    preflight = evaluate_rules(
        df,
        table_rules=(
            _non_empty_rule(normalized_name),
            _required_rule(normalized_name, contract.required_columns),
            _type_rule(normalized_name, contract.expected_types),
        ),
    )
    if preflight.hard_failures:
        return preflight
    business = evaluate_rules(
        df,
        row_rules=contract.row_rules(df),
        table_rules=contract.table_rules(df),
    )
    return _merge_results(preflight, business)

