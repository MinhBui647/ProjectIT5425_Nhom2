"""Tests for the dependency-free governance configuration and models."""

import json
import os
import subprocess
import sys
from datetime import datetime, timezone
from pathlib import Path

from data_governance import (
    CONTRACT_VERSION,
    DEFAULT_ERROR_THRESHOLD,
    BronzeIdentity,
    GateDecision,
    GateStatus,
    GovernanceRunResult,
    QualityIssue,
    QualityResult,
)
from data_governance.config import (
    DATA_DOCS_PATH,
    FIGURES_PATH,
    LINEAGE_SPOOL_PATH,
    QUALITY_REPORTS_PATH,
    QUARANTINE_PATH,
)


def test_governance_defaults_and_status_values():
    assert DEFAULT_ERROR_THRESHOLD == 0.005
    assert CONTRACT_VERSION == "v1"
    assert {status.value for status in GateStatus} == {
        "PASS",
        "PASS_WITH_REJECTIONS",
        "FAIL",
        "SKIPPED_DUPLICATE",
    }
    assert all(
        isinstance(path, Path)
        for path in (
            DATA_DOCS_PATH,
            FIGURES_PATH,
            LINEAGE_SPOOL_PATH,
            QUALITY_REPORTS_PATH,
            QUARANTINE_PATH,
        )
    )


def test_models_serialize_to_plain_dict_and_json(tmp_path):
    source_timestamp = datetime(2026, 10, 15, 3, 30, tzinfo=timezone.utc)
    identity = BronzeIdentity(
        absolute_path=(tmp_path / "sensor.parquet").resolve(),
        sha256="a" * 64,
        table_name="sensor_telemetry",
        row_count=2,
        source_timestamp=source_timestamp,
    )
    issue = QualityIssue(
        rule_id="sensor.uht_temp.range",
        severity="ERROR",
        message="Temperature is outside the accepted range",
        affected_row_indices=(1,),
        affected_row_count=1,
    )
    quality = QualityResult(
        total_row_count=2,
        invalid_row_mask=(False, True),
        rule_metrics={"sensor.uht_temp.range": {"failed": 1}},
        issues=(issue,),
    )
    decision = GateDecision(
        status=GateStatus.PASS_WITH_REJECTIONS,
        error_ratio=0.5,
        reason="One invalid row",
        fail_threshold=DEFAULT_ERROR_THRESHOLD,
    )
    result = GovernanceRunResult(
        run_id="run-001",
        status=decision.status,
        input_identity=identity,
        input_row_count=2,
        valid_row_count=1,
        rejected_row_count=1,
        report_paths={"summary": tmp_path / "summary.json"},
        lineage_status="COMPLETE",
    )

    assert quality.invalid_row_count == 1
    as_dict = result.to_dict()
    assert as_dict["status"] == "PASS_WITH_REJECTIONS"
    assert as_dict["input_identity"]["absolute_path"] == str(identity.absolute_path)
    assert as_dict["input_identity"]["source_timestamp"] == source_timestamp.isoformat()
    assert as_dict["report_paths"]["summary"] == str(tmp_path / "summary.json")
    assert json.loads(result.to_json()) == as_dict


def test_package_root_does_not_eagerly_import_optional_integrations():
    src_path = Path(__file__).resolve().parents[1] / "src"
    environment = os.environ.copy()
    environment["PYTHONPATH"] = str(src_path)
    script = (
        "import sys; import data_governance; "
        "assert 'great_expectations' not in sys.modules; "
        "assert 'openlineage' not in sys.modules"
    )

    completed = subprocess.run(
        [sys.executable, "-c", script],
        env=environment,
        check=False,
        capture_output=True,
        text=True,
    )

    assert completed.returncode == 0, completed.stderr

