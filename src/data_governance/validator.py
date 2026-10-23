"""Generic in-memory rule engine for governance contracts."""

from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Callable, Iterable

import polars as pl

from data_governance.models import QualityIssue, QualityResult


class DamaDimension(str, Enum):
    COMPLETENESS = "completeness"
    VALIDITY = "validity"
    UNIQUENESS = "uniqueness"
    CONSISTENCY = "consistency"
    TIMELINESS = "timeliness"
    ACCURACY = "accuracy"


class RuleSeverity(str, Enum):
    WARNING = "WARNING"
    ERROR = "ERROR"


@dataclass(frozen=True)
class RowRule:
    rule_id: str
    dimension: DamaDimension
    severity: RuleSeverity
    description: str
    failure_expression: Callable[[pl.DataFrame], pl.Expr]
    reject_rows: bool = True


@dataclass(frozen=True)
class TableRuleOutcome:
    passed: bool
    observed: dict[str, Any] = field(default_factory=dict)
    message: str | None = None


@dataclass(frozen=True)
class TableRule:
    rule_id: str
    dimension: DamaDimension
    severity: RuleSeverity
    description: str
    evaluator: Callable[[pl.DataFrame], TableRuleOutcome]
    hard_failure: bool = False


def _rule_metrics(
    *,
    dimension: DamaDimension,
    severity: RuleSeverity,
    description: str,
    **observed: Any,
) -> dict[str, Any]:
    return {
        "dimension": dimension.value,
        "severity": severity.value,
        "description": description,
        **observed,
    }


def evaluate_rules(
    df: pl.DataFrame,
    *,
    row_rules: Iterable[RowRule] = (),
    table_rules: Iterable[TableRule] = (),
) -> QualityResult:
    """Evaluate rules and merge row failures without double-counting rows."""
    total_rows = df.height
    invalid_mask = [False] * total_rows
    metrics: dict[str, Any] = {}
    issues: list[QualityIssue] = []
    hard_failures: list[str] = []

    for rule in table_rules:
        outcome = rule.evaluator(df)
        metrics[rule.rule_id] = _rule_metrics(
            dimension=rule.dimension,
            severity=rule.severity,
            description=rule.description,
            passed=outcome.passed,
            **outcome.observed,
        )
        if not outcome.passed:
            message = outcome.message or rule.description
            issues.append(
                QualityIssue(
                    rule_id=rule.rule_id,
                    severity=rule.severity.value,
                    message=message,
                )
            )
            if rule.hard_failure:
                hard_failures.append(rule.rule_id)

    for rule in row_rules:
        mask = (
            df.select(
                rule.failure_expression(df)
                .cast(pl.Boolean, strict=False)
                .fill_null(False)
                .alias("_failed")
            )
            .get_column("_failed")
            .to_list()
        )
        if len(mask) != total_rows:
            raise ValueError(
                f"Row rule {rule.rule_id} returned {len(mask)} values for "
                f"{total_rows} rows"
            )
        affected = tuple(index for index, failed in enumerate(mask) if failed)
        failed_count = len(affected)
        metrics[rule.rule_id] = _rule_metrics(
            dimension=rule.dimension,
            severity=rule.severity,
            description=rule.description,
            failed_count=failed_count,
            passed_count=total_rows - failed_count,
            failure_ratio=(failed_count / total_rows if total_rows else 0.0),
        )
        if affected:
            issues.append(
                QualityIssue(
                    rule_id=rule.rule_id,
                    severity=rule.severity.value,
                    message=rule.description,
                    affected_row_indices=affected,
                    affected_row_count=failed_count,
                )
            )
            if rule.reject_rows:
                for index in affected:
                    invalid_mask[index] = True

    return QualityResult(
        total_row_count=total_rows,
        invalid_row_mask=tuple(invalid_mask),
        rule_metrics=metrics,
        hard_failures=tuple(hard_failures),
        issues=tuple(issues),
    )

