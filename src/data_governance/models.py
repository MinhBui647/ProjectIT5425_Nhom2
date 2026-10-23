"""Dependency-free data models shared by the governance pipeline."""

from __future__ import annotations

import json
from dataclasses import asdict, dataclass, field
from datetime import date, datetime
from enum import Enum
from pathlib import Path
from typing import Any


class GateStatus(str, Enum):
    """Possible outcomes of a governance quality gate."""

    PASS = "PASS"
    PASS_WITH_REJECTIONS = "PASS_WITH_REJECTIONS"
    FAIL = "FAIL"
    SKIPPED_DUPLICATE = "SKIPPED_DUPLICATE"


def _json_value(value: Any) -> Any:
    """Convert nested dataclass values to JSON-compatible primitives."""
    if isinstance(value, Enum):
        return value.value
    if isinstance(value, Path):
        return str(value)
    if isinstance(value, (datetime, date)):
        return value.isoformat()
    if isinstance(value, dict):
        return {str(key): _json_value(item) for key, item in value.items()}
    if isinstance(value, (list, tuple)):
        return [_json_value(item) for item in value]
    return value


class SerializableModel:
    """Small serialization mixin without optional runtime dependencies."""

    def to_dict(self) -> dict[str, Any]:
        return _json_value(asdict(self))

    def to_json(self, **kwargs: Any) -> str:
        options = {"ensure_ascii": False}
        options.update(kwargs)
        return json.dumps(self.to_dict(), **options)


@dataclass(frozen=True)
class BronzeIdentity(SerializableModel):
    absolute_path: Path
    sha256: str
    table_name: str
    row_count: int
    source_timestamp: datetime | None = None


@dataclass(frozen=True)
class QualityIssue(SerializableModel):
    rule_id: str
    severity: str
    message: str
    affected_row_indices: tuple[int, ...] = ()
    affected_row_count: int = 0


@dataclass(frozen=True)
class QualityResult(SerializableModel):
    total_row_count: int
    invalid_row_mask: tuple[bool, ...] = ()
    rule_metrics: dict[str, Any] = field(default_factory=dict)
    hard_failures: tuple[str, ...] = ()
    issues: tuple[QualityIssue, ...] = ()

    @property
    def invalid_row_count(self) -> int:
        return sum(self.invalid_row_mask)

    @property
    def error_ratio(self) -> float:
        if self.total_row_count == 0:
            return 0.0
        return self.invalid_row_count / self.total_row_count

    def failed_rules_for_row(self, row_index: int) -> tuple[str, ...]:
        """Return every row-rule id that affected a given zero-based row."""
        return tuple(
            issue.rule_id
            for issue in self.issues
            if row_index in issue.affected_row_indices
        )


@dataclass(frozen=True)
class GateDecision(SerializableModel):
    status: GateStatus
    error_ratio: float
    reason: str
    fail_threshold: float
    valid_row_count: int = 0
    rejected_row_count: int = 0
    hard_failure_count: int = 0


@dataclass(frozen=True)
class GovernanceRunResult(SerializableModel):
    run_id: str
    status: GateStatus
    input_identity: BronzeIdentity
    output_identity: dict[str, Any] | None = None
    input_row_count: int = 0
    valid_row_count: int = 0
    rejected_row_count: int = 0
    report_paths: dict[str, Path] = field(default_factory=dict)
    lineage_status: str | None = None

