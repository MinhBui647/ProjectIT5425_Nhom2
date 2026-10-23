"""Local Delta tests for governed Silver commits and idempotency."""

from datetime import datetime, timedelta, timezone

import polars as pl
import pytest
from deltalake import DeltaTable

from data_governance.gatekeeper import apply_gate
from data_governance.models import BronzeIdentity, GateStatus, QualityIssue, QualityResult
from data_governance.silver_writer import already_ingested, write_governed_silver


def _sensor_frame(row_count):
    timestamps = pl.Series(
        "timestamp",
        [
            datetime(2026, 1, 1, tzinfo=timezone.utc) + timedelta(seconds=index)
            for index in range(row_count)
        ],
    ).cast(pl.Datetime("us", "UTC"))
    return pl.DataFrame(
        {
            "timestamp": timestamps,
            "line_id": ["LINE_A"] * row_count,
            "batch_id": [None] * row_count,
            "preheat_temp": pl.Series([75.0] * row_count, dtype=pl.Float32),
            "uht_temp": pl.Series([140.0] * row_count, dtype=pl.Float32),
            "homo_press_stage1": pl.Series([200.0] * row_count, dtype=pl.Float32),
            "homo_press_stage2": pl.Series([30.0] * row_count, dtype=pl.Float32),
            "flow_rate": pl.Series([5000.0] * row_count, dtype=pl.Float32),
            "conductivity": pl.Series([4.5] * row_count, dtype=pl.Float32),
            "power_kw": pl.Series([150.0] * row_count, dtype=pl.Float32),
            "is_anomaly_flag": [False] * row_count,
            "ingestion_date": [datetime(2026, 1, 1).date()] * row_count,
            "_source_system": ["TEST"] * row_count,
        }
    )


def _quality(total, invalid_indices=(), hard=False):
    invalid = set(invalid_indices)
    issues = (
        QualityIssue(
            rule_id="sensor.demo.v1",
            severity="ERROR",
            message="Synthetic",
            affected_row_indices=tuple(invalid_indices),
            affected_row_count=len(invalid_indices),
        ),
    ) if invalid_indices else ()
    return QualityResult(
        total_row_count=total,
        invalid_row_mask=tuple(index in invalid for index in range(total)),
        hard_failures=("sensor.schema.v1",) if hard else (),
        issues=issues,
    )


def _identity(tmp_path, row_count, digest="a" * 64):
    return BronzeIdentity(
        absolute_path=(tmp_path / "sensor.parquet").resolve(),
        sha256=digest,
        table_name="sensor_telemetry",
        row_count=row_count,
        source_timestamp=datetime(2026, 1, 1, tzinfo=timezone.utc),
    )


def test_same_table_sha_and_contract_version_is_skipped(tmp_path):
    table_uri = str(tmp_path / "silver_sensor")
    frame = _sensor_frame(2)
    valid, _, decision = apply_gate(frame, _quality(2))
    identity = _identity(tmp_path, 2)

    first = write_governed_silver(
        valid,
        table_uri=table_uri,
        table_name="sensor_telemetry",
        bronze_identity=identity,
        decision=decision,
        run_id="run-001",
    )
    version_after_first = DeltaTable(table_uri).version()
    second = write_governed_silver(
        valid,
        table_uri=table_uri,
        table_name="sensor_telemetry",
        bronze_identity=identity,
        decision=decision,
        run_id="run-002",
    )
    stored = pl.read_delta(table_uri)

    assert first["written"] is True
    assert first["rows_written"] == 2
    assert second["status"] is GateStatus.SKIPPED_DUPLICATE
    assert DeltaTable(table_uri).version() == version_after_first
    assert stored.height == 2
    assert stored["_bronze_sha256"].unique().to_list() == ["a" * 64]
    assert stored["_quality_status"].unique().to_list() == ["PASS"]
    assert stored["_quality_run_id"].unique().to_list() == ["run-001"]
    assert stored["_contract_version"].unique().to_list() == ["v1"]
    assert already_ingested(table_uri, "a" * 64, "v1")


def test_same_table_and_sha_with_new_contract_version_is_processed(tmp_path):
    table_uri = str(tmp_path / "silver_versioned")
    frame = _sensor_frame(2)
    valid, _, decision = apply_gate(frame, _quality(2))
    identity = _identity(tmp_path, 2)

    first = write_governed_silver(
        valid,
        table_uri=table_uri,
        table_name="sensor_telemetry",
        bronze_identity=identity,
        decision=decision,
        run_id="run-v1",
        contract_version="v1",
    )
    second = write_governed_silver(
        valid,
        table_uri=table_uri,
        table_name="sensor_telemetry",
        bronze_identity=identity,
        decision=decision,
        run_id="run-v2",
        contract_version="v2",
    )
    stored = pl.read_delta(table_uri)

    assert first["written"] is True
    assert second["written"] is True
    assert stored.height == 4
    assert set(stored["_contract_version"].to_list()) == {"v1", "v2"}
    assert already_ingested(table_uri, identity.sha256, "v1")
    assert already_ingested(table_uri, identity.sha256, "v2")


def test_different_table_with_same_sha_is_processed(tmp_path):
    frame = _sensor_frame(2)
    valid, _, decision = apply_gate(frame, _quality(2))
    identity = _identity(tmp_path, 2)
    first_uri = str(tmp_path / "silver_table_a")
    second_uri = str(tmp_path / "silver_table_b")

    first = write_governed_silver(
        valid,
        table_uri=first_uri,
        table_name="sensor_telemetry",
        bronze_identity=identity,
        decision=decision,
        run_id="run-table-a",
    )
    second = write_governed_silver(
        valid,
        table_uri=second_uri,
        table_name="sensor_telemetry",
        bronze_identity=identity,
        decision=decision,
        run_id="run-table-b",
    )

    assert first["written"] is True
    assert second["written"] is True
    assert pl.read_delta(first_uri).height == 2
    assert pl.read_delta(second_uri).height == 2


def test_missing_contract_version_fails_versioned_idempotency(tmp_path):
    table_uri = str(tmp_path / "legacy_silver")
    pl.DataFrame(
        {
            "_bronze_sha256": ["a" * 64],
            "value": [1],
        }
    ).write_delta(table_uri)

    with pytest.raises(ValueError, match="missing _contract_version"):
        already_ingested(table_uri, "a" * 64, "v1")


def test_pass_with_rejections_writes_only_valid_rows(tmp_path):
    table_uri = str(tmp_path / "silver_rejections")
    frame = _sensor_frame(200)
    valid, invalid, decision = apply_gate(frame, _quality(200, (7,)))

    result = write_governed_silver(
        valid,
        table_uri=table_uri,
        table_name="sensor_telemetry",
        bronze_identity=_identity(tmp_path, 200, "b" * 64),
        decision=decision,
        run_id="run-reject",
    )

    assert decision.status is GateStatus.PASS_WITH_REJECTIONS
    assert invalid.height == 1
    assert result["rows_written"] == 199
    assert pl.read_delta(table_uri).height == 199


def test_fail_dry_run_and_duplicate_do_not_change_silver(tmp_path):
    frame = _sensor_frame(2)
    failed_valid, _, failed_decision = apply_gate(frame, _quality(2, hard=True))
    fail_uri = str(tmp_path / "silver_fail")
    dry_uri = str(tmp_path / "silver_dry")

    failed = write_governed_silver(
        failed_valid,
        table_uri=fail_uri,
        table_name="sensor_telemetry",
        bronze_identity=_identity(tmp_path, 2),
        decision=failed_decision,
        run_id="run-fail",
    )
    _, _, pass_decision = apply_gate(frame, _quality(2))
    dry = write_governed_silver(
        frame,
        table_uri=dry_uri,
        table_name="sensor_telemetry",
        bronze_identity=_identity(tmp_path, 2),
        decision=pass_decision,
        run_id="run-dry",
        dry_run=True,
    )

    assert failed["written"] is False
    assert dry["written"] is False
    assert not (tmp_path / "silver_fail").exists()
    assert not (tmp_path / "silver_dry").exists()

