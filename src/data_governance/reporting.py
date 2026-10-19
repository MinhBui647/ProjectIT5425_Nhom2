"""Atomic machine-readable quality and quarantine artifacts."""

from __future__ import annotations

import json
import os
import re
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import polars as pl

from data_governance.config import (
    CONTRACT_VERSION,
    QUALITY_REPORTS_PATH,
    QUARANTINE_PATH,
)
from data_governance.models import (
    BronzeIdentity,
    GateDecision,
    QualityResult,
)
from lakehouse_storage.utils import sha256_file


_SAFE_COMPONENT = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._-]*$")


def _validate_component(value: str, label: str) -> str:
    if not _SAFE_COMPONENT.fullmatch(value):
        raise ValueError(f"Unsafe {label}: {value!r}")
    return value


def _atomic_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(f".{path.name}.tmp-{uuid.uuid4().hex}")
    try:
        temporary.write_text(
            json.dumps(payload, ensure_ascii=False, indent=2),
            encoding="utf-8",
        )
        os.replace(temporary, path)
    finally:
        temporary.unlink(missing_ok=True)


def _atomic_parquet(path: Path, frame: pl.DataFrame) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(f".{path.name}.tmp-{uuid.uuid4().hex}")
    try:
        frame.write_parquet(temporary)
        os.replace(temporary, path)
    finally:
        temporary.unlink(missing_ok=True)


def write_failure_artifact(
    *,
    run_id: str,
    table_name: str,
    bronze_path: str | Path,
    failure_stage: str,
    error_category: str,
    reason: str,
    started_at: datetime,
    finished_at: datetime | None = None,
    reports_root: str | Path = QUALITY_REPORTS_PATH,
) -> Path:
    """Persist a minimal audit record when canonical validation cannot run.

    Row counts, rule metrics, and quarantine are deliberately unknown at this
    point. Keeping them as ``null`` avoids presenting an unreadable Bronze file
    as a successfully inspected batch with zero rows.
    """
    run_id = _validate_component(run_id, "run_id")
    table_name = _validate_component(table_name, "table_name")
    failure_stage = _validate_component(failure_stage, "failure_stage")
    error_category = _validate_component(error_category, "error_category")
    finished_at = finished_at or datetime.now(timezone.utc)
    if started_at.tzinfo is None or finished_at.tzinfo is None:
        raise ValueError("Artifact timestamps must be timezone-aware")

    summary_path = Path(reports_root) / run_id / "summary.json"
    payload = {
        "run_id": run_id,
        "table_name": table_name,
        "contract_version": CONTRACT_VERSION,
        "bronze": {
            "absolute_path": str(Path(bronze_path).resolve()),
            "sha256": None,
            "row_count": None,
        },
        "started_at": started_at.astimezone(timezone.utc).isoformat(),
        "finished_at": finished_at.astimezone(timezone.utc).isoformat(),
        "counts": {"total": None, "valid": None, "rejected": None},
        "error_ratio": None,
        "decision": {
            "status": "FAIL",
            "reason": reason,
        },
        "error_category": error_category,
        "failure_stage": failure_stage,
        "hard_failures": [],
        "rule_metrics": {},
        "artifacts": {
            "quarantine": None,
            "quarantine_sha256": None,
        },
    }
    _atomic_json(summary_path, payload)
    return summary_path


def write_quality_artifacts(
    invalid_df: pl.DataFrame,
    *,
    run_id: str,
    table_name: str,
    bronze_identity: BronzeIdentity,
    quality: QualityResult,
    decision: GateDecision,
    started_at: datetime,
    finished_at: datetime | None = None,
    reports_root: str | Path = QUALITY_REPORTS_PATH,
    quarantine_root: str | Path = QUARANTINE_PATH,
) -> dict[str, Path]:
    """Persist a summary and optional rejected rows using atomic replacement."""
    run_id = _validate_component(run_id, "run_id")
    table_name = _validate_component(table_name, "table_name")
    if invalid_df.height != decision.rejected_row_count:
        raise ValueError(
            "Invalid dataframe row count does not match gate decision: "
            f"{invalid_df.height} != {decision.rejected_row_count}"
        )

    finished_at = finished_at or datetime.now(timezone.utc)
    if started_at.tzinfo is None or finished_at.tzinfo is None:
        raise ValueError("Artifact timestamps must be timezone-aware")

    report_dir = Path(reports_root) / run_id
    summary_path = report_dir / "summary.json"
    paths: dict[str, Path] = {"summary": summary_path}
    quarantine_sha256: str | None = None

    if not invalid_df.is_empty():
        partition_date = finished_at.astimezone(timezone.utc).date().isoformat()
        quarantine_path = (
            Path(quarantine_root)
            / table_name
            / partition_date
            / run_id
            / "invalid_rows.parquet"
        )
        quarantine = invalid_df.with_columns(
            pl.lit(run_id).alias("_quality_run_id"),
            pl.lit(str(bronze_identity.absolute_path)).alias("_bronze_file"),
            pl.lit(bronze_identity.sha256).alias("_bronze_sha256"),
            pl.lit(CONTRACT_VERSION).alias("_contract_version"),
        )
        _atomic_parquet(quarantine_path, quarantine)
        quarantine_sha256 = sha256_file(quarantine_path)
        paths["quarantine"] = quarantine_path

    payload = {
        "run_id": run_id,
        "table_name": table_name,
        "contract_version": CONTRACT_VERSION,
        "bronze": bronze_identity.to_dict(),
        "started_at": started_at.astimezone(timezone.utc).isoformat(),
        "finished_at": finished_at.astimezone(timezone.utc).isoformat(),
        "counts": {
            "total": quality.total_row_count,
            "valid": decision.valid_row_count,
            "rejected": decision.rejected_row_count,
        },
        "error_ratio": quality.error_ratio,
        "decision": decision.to_dict(),
        "hard_failures": list(quality.hard_failures),
        "rule_metrics": quality.to_dict()["rule_metrics"],
        "artifacts": {
            "quarantine": str(paths["quarantine"]) if "quarantine" in paths else None,
            "quarantine_sha256": quarantine_sha256,
        },
    }
    _atomic_json(summary_path, payload)
    return paths

