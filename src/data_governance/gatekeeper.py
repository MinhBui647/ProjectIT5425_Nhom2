"""Pure in-memory quality gate and valid/rejected row splitter."""

from __future__ import annotations

import polars as pl

from data_governance.config import DEFAULT_ERROR_THRESHOLD
from data_governance.models import GateDecision, GateStatus, QualityResult


def _split_rows(
    df: pl.DataFrame,
    quality: QualityResult,
) -> tuple[pl.DataFrame, pl.DataFrame]:
    indexed = df.with_row_index("_quality_row_index").with_columns(
        pl.Series("_invalid_row", quality.invalid_row_mask, dtype=pl.Boolean)
    )
    valid = indexed.filter(~pl.col("_invalid_row")).drop(
        "_quality_row_index", "_invalid_row"
    )
    invalid_indexed = indexed.filter(pl.col("_invalid_row"))
    failed_rules = [
        list(quality.failed_rules_for_row(index))
        for index in invalid_indexed.get_column("_quality_row_index").to_list()
    ]
    invalid = invalid_indexed.drop("_quality_row_index", "_invalid_row").with_columns(
        pl.Series("_failed_rules", failed_rules, dtype=pl.List(pl.String))
    )
    return valid, invalid


def apply_gate(
    df: pl.DataFrame,
    quality: QualityResult,
    fail_threshold: float = DEFAULT_ERROR_THRESHOLD,
    *,
    is_duplicate: bool = False,
) -> tuple[pl.DataFrame, pl.DataFrame, GateDecision]:
    """Split rows and decide whether a canonical batch may proceed to Silver."""
    if not 0 <= fail_threshold <= 1:
        raise ValueError("fail_threshold must be between 0 and 1")
    if quality.total_row_count != df.height:
        raise ValueError(
            "QualityResult row count does not match dataframe: "
            f"{quality.total_row_count} != {df.height}"
        )
    if len(quality.invalid_row_mask) != df.height:
        raise ValueError("QualityResult invalid_row_mask length does not match dataframe")

    if is_duplicate:
        empty = df.head(0)
        return empty, empty, GateDecision(
            status=GateStatus.SKIPPED_DUPLICATE,
            error_ratio=quality.error_ratio,
            reason="Bronze checksum was already processed",
            fail_threshold=fail_threshold,
            hard_failure_count=len(quality.hard_failures),
        )

    valid, invalid = _split_rows(df, quality)
    hard_failure_count = len(quality.hard_failures)
    if df.is_empty():
        status = GateStatus.FAIL
        reason = "Input dataframe is empty"
    elif hard_failure_count:
        status = GateStatus.FAIL
        reason = f"Hard quality failures: {list(quality.hard_failures)}"
    elif quality.error_ratio > fail_threshold:
        status = GateStatus.FAIL
        reason = (
            f"Error ratio {quality.error_ratio:.6f} exceeds "
            f"threshold {fail_threshold:.6f}"
        )
    elif quality.invalid_row_count:
        status = GateStatus.PASS_WITH_REJECTIONS
        reason = (
            f"Rejected {quality.invalid_row_count} rows within "
            f"threshold {fail_threshold:.6f}"
        )
    else:
        status = GateStatus.PASS
        reason = "All rows passed quality checks"

    decision = GateDecision(
        status=status,
        error_ratio=quality.error_ratio,
        reason=reason,
        fail_threshold=fail_threshold,
        valid_row_count=valid.height,
        rejected_row_count=invalid.height,
        hard_failure_count=hard_failure_count,
    )
    return valid, invalid, decision

