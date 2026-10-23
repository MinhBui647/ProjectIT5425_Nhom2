"""Boundary and purity tests for the in-memory governance gatekeeper."""

import polars as pl
import pytest

from data_governance.gatekeeper import apply_gate
from data_governance.models import GateStatus, QualityIssue, QualityResult


def _quality(total: int, invalid_count: int, *, hard_failure: bool = False):
    affected = tuple(range(invalid_count))
    issues = (
        QualityIssue(
            rule_id="demo.rule.v1",
            severity="ERROR",
            message="Synthetic failure",
            affected_row_indices=affected,
            affected_row_count=invalid_count,
        ),
    ) if invalid_count else ()
    return QualityResult(
        total_row_count=total,
        invalid_row_mask=tuple(index < invalid_count for index in range(total)),
        hard_failures=("demo.hard.v1",) if hard_failure else (),
        issues=issues,
    )


@pytest.mark.parametrize(
    ("invalid_count", "expected_status"),
    [
        (0, GateStatus.PASS),
        (4, GateStatus.PASS_WITH_REJECTIONS),
        (5, GateStatus.PASS_WITH_REJECTIONS),
        (6, GateStatus.FAIL),
    ],
)
def test_gate_threshold_boundaries(invalid_count, expected_status):
    frame = pl.DataFrame({"row_id": range(1000)})

    valid, invalid, decision = apply_gate(frame, _quality(1000, invalid_count))

    assert decision.status is expected_status
    assert decision.error_ratio == invalid_count / 1000
    assert valid.height == 1000 - invalid_count
    assert invalid.height == invalid_count
    assert decision.valid_row_count == valid.height
    assert decision.rejected_row_count == invalid.height


def test_hard_failure_always_fails_even_with_zero_bad_rows():
    frame = pl.DataFrame({"row_id": [1, 2]})

    valid, invalid, decision = apply_gate(
        frame,
        _quality(2, 0, hard_failure=True),
    )

    assert decision.status is GateStatus.FAIL
    assert decision.hard_failure_count == 1
    assert valid.height == 2
    assert invalid.is_empty()


def test_invalid_rows_include_every_failed_rule():
    frame = pl.DataFrame({"row_id": [0, 1, 2]})
    quality = QualityResult(
        total_row_count=3,
        invalid_row_mask=(False, True, False),
        issues=(
            QualityIssue(
                rule_id="demo.first.v1",
                severity="ERROR",
                message="First",
                affected_row_indices=(1,),
                affected_row_count=1,
            ),
            QualityIssue(
                rule_id="demo.second.v1",
                severity="ERROR",
                message="Second",
                affected_row_indices=(1,),
                affected_row_count=1,
            ),
        ),
    )

    _, invalid, _ = apply_gate(frame, quality, fail_threshold=0.5)

    assert invalid["row_id"].to_list() == [1]
    assert invalid["_failed_rules"].to_list() == [
        ["demo.first.v1", "demo.second.v1"]
    ]


def test_duplicate_status_returns_no_rows_for_processing():
    frame = pl.DataFrame({"row_id": [1, 2]})

    valid, invalid, decision = apply_gate(
        frame,
        _quality(2, 0),
        is_duplicate=True,
    )

    assert decision.status is GateStatus.SKIPPED_DUPLICATE
    assert valid.is_empty()
    assert invalid.is_empty()


def test_empty_input_fails_and_gatekeeper_performs_no_io(tmp_path):
    before = list(tmp_path.iterdir())

    _, _, decision = apply_gate(pl.DataFrame({"row_id": []}), _quality(0, 0))

    assert decision.status is GateStatus.FAIL
    assert list(tmp_path.iterdir()) == before


def test_gate_rejects_inconsistent_quality_result():
    with pytest.raises(ValueError, match="row count does not match"):
        apply_gate(pl.DataFrame({"row_id": [1]}), _quality(2, 0))

