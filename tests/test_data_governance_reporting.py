"""Tests for atomic governance quality artifacts."""

import json
from datetime import datetime, timezone

import polars as pl

from data_governance.gatekeeper import apply_gate
from data_governance.models import BronzeIdentity, QualityIssue, QualityResult
from data_governance.reporting import write_failure_artifact, write_quality_artifacts
from lakehouse_storage.utils import sha256_file


NOW = datetime(2026, 10, 15, 12, 0, tzinfo=timezone.utc)


def _identity(tmp_path, row_count):
    return BronzeIdentity(
        absolute_path=(tmp_path / "sensor.parquet").resolve(),
        sha256="a" * 64,
        table_name="sensor_telemetry",
        row_count=row_count,
        source_timestamp=NOW,
    )


def _quality(total, invalid_indices=(), hard_failures=()):
    invalid_set = set(invalid_indices)
    issues = (
        QualityIssue(
            rule_id="sensor.demo.v1",
            severity="ERROR",
            message="Synthetic error",
            affected_row_indices=tuple(invalid_indices),
            affected_row_count=len(invalid_indices),
        ),
    ) if invalid_indices else ()
    return QualityResult(
        total_row_count=total,
        invalid_row_mask=tuple(index in invalid_set for index in range(total)),
        rule_metrics={"sensor.demo.v1": {"failed_count": len(invalid_indices)}},
        hard_failures=tuple(hard_failures),
        issues=issues,
    )


def test_pass_writes_summary_without_empty_quarantine(tmp_path):
    frame = pl.DataFrame({"row_id": [1, 2]})
    quality = _quality(2)
    _, invalid, decision = apply_gate(frame, quality)

    paths = write_quality_artifacts(
        invalid,
        run_id="run-pass",
        table_name="sensor_telemetry",
        bronze_identity=_identity(tmp_path, 2),
        quality=quality,
        decision=decision,
        started_at=NOW,
        finished_at=NOW,
        reports_root=tmp_path / "reports",
        quarantine_root=tmp_path / "quarantine",
    )
    summary = json.loads(paths["summary"].read_text(encoding="utf-8"))

    assert set(paths) == {"summary"}
    assert summary["decision"]["status"] == "PASS"
    assert summary["counts"] == {"total": 2, "valid": 2, "rejected": 0}
    assert summary["bronze"]["sha256"] == "a" * 64
    assert summary["artifacts"]["quarantine"] is None
    assert not (tmp_path / "quarantine").exists()


def test_rejections_write_partitioned_parquet_and_complete_summary(tmp_path):
    frame = pl.DataFrame({"row_id": range(200)})
    quality = _quality(200, (7,))
    _, invalid, decision = apply_gate(frame, quality)

    paths = write_quality_artifacts(
        invalid,
        run_id="run-reject",
        table_name="sensor_telemetry",
        bronze_identity=_identity(tmp_path, 200),
        quality=quality,
        decision=decision,
        started_at=NOW,
        finished_at=NOW,
        reports_root=tmp_path / "reports",
        quarantine_root=tmp_path / "quarantine",
    )
    quarantined = pl.read_parquet(paths["quarantine"])
    summary = json.loads(paths["summary"].read_text(encoding="utf-8"))

    assert paths["quarantine"].parts[-4:] == (
        "sensor_telemetry",
        "2026-10-15",
        "run-reject",
        "invalid_rows.parquet",
    )
    assert quarantined.height == 1
    assert quarantined["row_id"].to_list() == [7]
    assert quarantined["_failed_rules"].to_list() == [["sensor.demo.v1"]]
    assert quarantined["_quality_run_id"].to_list() == ["run-reject"]
    assert quarantined["_bronze_sha256"].to_list() == ["a" * 64]
    assert summary["artifacts"]["quarantine_sha256"] == sha256_file(
        paths["quarantine"]
    )
    assert summary["counts"]["rejected"] == 1
    assert summary["error_ratio"] == 0.005


def test_hard_fail_still_writes_report_and_leaves_no_temporary_files(tmp_path):
    frame = pl.DataFrame({"row_id": [1]})
    quality = _quality(1, hard_failures=("sensor.schema.v1",))
    _, invalid, decision = apply_gate(frame, quality)

    paths = write_quality_artifacts(
        invalid,
        run_id="run-fail",
        table_name="sensor_telemetry",
        bronze_identity=_identity(tmp_path, 1),
        quality=quality,
        decision=decision,
        started_at=NOW,
        finished_at=NOW,
        reports_root=tmp_path / "reports",
        quarantine_root=tmp_path / "quarantine",
    )
    summary = json.loads(paths["summary"].read_text(encoding="utf-8"))

    assert summary["decision"]["status"] == "FAIL"
    assert summary["hard_failures"] == ["sensor.schema.v1"]
    assert not list(tmp_path.rglob("*.tmp-*"))


def test_early_failure_writes_minimal_audit_without_fake_row_counts(tmp_path):
    bronze_path = tmp_path / "bronze" / "sensor.parquet"
    summary_path = write_failure_artifact(
        run_id="run-checksum-fail",
        table_name="sensor_telemetry",
        bronze_path=bronze_path,
        failure_stage="load_bronze",
        error_category="quality",
        reason="Checksum mismatch",
        started_at=NOW,
        finished_at=NOW,
        reports_root=tmp_path / "reports",
    )
    summary = json.loads(summary_path.read_text(encoding="utf-8"))

    assert summary["decision"] == {
        "status": "FAIL",
        "reason": "Checksum mismatch",
    }
    assert summary["failure_stage"] == "load_bronze"
    assert summary["error_category"] == "quality"
    assert summary["counts"] == {
        "total": None,
        "valid": None,
        "rejected": None,
    }
    assert summary["bronze"]["sha256"] is None
    assert summary["artifacts"]["quarantine"] is None
    assert not list(tmp_path.rglob("*.tmp-*"))

