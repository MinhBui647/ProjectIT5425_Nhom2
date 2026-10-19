import json
from pathlib import Path

from data_governance.lineage import LineageEmitter, build_run_event, event_to_dict


def _event(event_type="START"):
    return build_run_event(
        event_type,
        run_id="087ab4c0-68e1-4fe0-b50b-aeae7db00d7f",
        table_name="sensor_telemetry",
        bronze_path=Path("bronze/sensor_telemetry/sample.parquet"),
        bronze_root=Path("bronze"),
        event_time="2026-10-16T08:00:00Z",
    )


def test_event_shape_matches_project_contract():
    payload = event_to_dict(_event("COMPLETE"))

    assert payload["eventType"] == "COMPLETE"
    assert payload["run"]["runId"] == "087ab4c0-68e1-4fe0-b50b-aeae7db00d7f"
    assert payload["job"]["namespace"] == "smart-dairy-factory"
    assert payload["job"]["name"] == "bronze_to_silver.sensor_telemetry"
    assert payload["inputs"][0]["namespace"] == "file://bronze"
    assert payload["inputs"][0]["name"] == "sensor_telemetry/sample.parquet"
    assert payload["outputs"][0]["namespace"] == "s3://smart-dairy-lakehouse"
    assert payload["outputs"][0]["name"] == "02_silver_curated/sensor_telemetry"


def test_emit_failure_spools_api_ready_json(tmp_path):
    def fail(_event):
        raise ConnectionError("offline")

    emitter = LineageEmitter(spool_path=tmp_path, emit_func=fail)
    assert emitter.emit(_event()) == "SPOOLED"
    files = list(tmp_path.rglob("*.json"))
    assert len(files) == 1
    assert json.loads(files[0].read_text(encoding="utf-8"))["eventType"] == "START"


def test_replay_deletes_only_successfully_emitted_files(tmp_path):
    failed = LineageEmitter(
        spool_path=tmp_path,
        emit_func=lambda _event: (_ for _ in ()).throw(ConnectionError("offline")),
    )
    failed.spool(_event("START"))
    failed.spool(_event("COMPLETE"))

    calls = []

    def partly_available(event):
        calls.append(event.eventType.value)
        if event.eventType.value == "START":
            raise ConnectionError("still offline")

    result = LineageEmitter(spool_path=tmp_path, emit_func=partly_available).replay()
    assert result == {"emitted": 1, "failed": 1, "remaining": 1}
    assert calls == ["COMPLETE", "START"]
    remaining = json.loads(next(tmp_path.rglob("*.json")).read_text(encoding="utf-8"))
    assert remaining["eventType"] == "START"


def test_s3_input_namespace_and_relative_name():
    event = build_run_event(
        "FAIL",
        run_id="087ab4c0-68e1-4fe0-b50b-aeae7db00d7f",
        table_name="weather",
        bronze_path="s3://smart-dairy-lakehouse/01_bronze_vault/weather/a.parquet",
        bronze_root="s3://smart-dairy-lakehouse/01_bronze_vault",
    )
    payload = event_to_dict(event)
    assert payload["inputs"][0]["namespace"] == (
        "s3://smart-dairy-lakehouse/01_bronze_vault"
    )
    assert payload["inputs"][0]["name"] == "weather/a.parquet"
