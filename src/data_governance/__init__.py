"""Data governance primitives for the Bronze-to-Silver pipeline.

The package root intentionally exposes only dependency-free configuration and
models. Optional GX and OpenLineage integrations are imported by their own
modules when used.
"""

from data_governance.config import CONTRACT_VERSION, DEFAULT_ERROR_THRESHOLD
from data_governance.models import (
    BronzeIdentity,
    GateDecision,
    GateStatus,
    GovernanceRunResult,
    QualityIssue,
    QualityResult,
)

__all__ = [
    "BronzeIdentity",
    "CONTRACT_VERSION",
    "DEFAULT_ERROR_THRESHOLD",
    "GateDecision",
    "GateStatus",
    "GovernanceRunResult",
    "QualityIssue",
    "QualityResult",
]

