"""Local acceptance: real Bronze, Delta, quality, docs, figures, and lineage."""

from datetime import datetime, timedelta, timezone
from pathlib import Path

import polars as pl
from deltalake import DeltaTable

from data_governance.run_governance import build_parser, run_governance
from data_governance.lineage import LineageEmitter
from lakehouse_storage.bronze_writer import write_bronze_batch
from lakehouse_storage.schemas import get_silver_schema


def _sensor_frame(rows: int, invalid_rows: int = 0) -> pl.DataFrame:
    base = datetime(2026, 10, 16, tzinfo=timezone.utc)
    uht = [200.0 if index < invalid_rows else 140.0 for index in range(rows)]
    return pl.DataFrame(
        {
            "timestamp": [(base + timedelta(seconds=index)).isoformat() for index in range(rows)],
            "line_id": ["LINE_UHT_1"] * rows,
            "batch_id": [f"BATCH_{index // 20:03d}" for index in range(rows)],
            "preheat_temp": [75.0] * rows,
            "uht_temp": uht,
            "homo_press_stage1": [200.0] * rows,
            "homo_press_stage2": [30.0] * rows,
            "flow_rate": [5000.0] * rows,
            "conductivity": [4.5] * rows,
            "power_kw": [150.0] * rows,
            "_source_system": ["S24_ACCEPTANCE"] * rows,
            "_generated_at": [base.isoformat()] * rows,
        }
    )


def _write_case(root: Path, rows: int, invalid_rows: int = 0) -> Path:
    return Path(
        write_bronze_batch(
            _sensor_frame(rows, invalid_rows),
            "sensor_telemetry",
            bronze_root=root,
        )
    )


def _run(
    bronze_root: Path,
    silver_root: Path,
    artifacts_root: Path,
    *,
    docs: bool = False,
):
    arguments = [
        "--table",
        "sensor_telemetry",
        "--bronze-root",
        str(bronze_root),
        "--silver-root",
        str(silver_root),
        "--lineage-endpoint",
        "http://127.0.0.1:9",
        "--lineage-spool",
        str(artifacts_root / "lineage_spool"),
        "--reports-root",
        str(artifacts_root / "data_quality"),
        "--quarantine-root",
        str(artifacts_root / "quarantine"),
        "--figures-root",
        str(artifacts_root / "figures"),
        "--gx-project-root",
        str(artifacts_root / "gx"),
    ]
    if docs:
        arguments.append("--build-data-docs")
    namespace = build_parser().parse_args(arguments)
    return run_governance(namespace)


def test_local_end_to_end_acceptance(tmp_path, monkeypatch):
    def offline(_self, _event):
        raise ConnectionError("offline acceptance transport")

    monkeypatch.setattr(LineageEmitter, "_emit", offline)
    silver_root = tmp_path / "silver"

    clean_bronze = tmp_path / "clean_bronze"
    clean_artifacts = tmp_path / "clean_artifacts"
    clean_path = _write_case(clean_bronze, 200)
    clean_summary, clean_exit = _run(
        clean_bronze, silver_root, clean_artifacts, docs=True
    )
    clean = clean_summary["results"][0]
    table_uri = silver_root / "sensor_telemetry"
    stored = pl.read_delta(table_uri)

    assert clean_exit == 0
    assert clean["status"] == "PASS"
    assert clean["rows_written"] == 200
    assert stored.height == 200
    contract_columns = set(get_silver_schema("sensor_telemetry").names)
    assert contract_columns.issubset(stored.columns)
    assert {
        "_bronze_file",
        "_bronze_sha256",
        "_quality_status",
        "_quality_run_id",
        "_contract_version",
        "ingestion_date",
    }.issubset(stored.columns)
    assert stored["_bronze_file"].unique().to_list() == [str(clean_path.resolve())]
    assert list(table_uri.rglob("ingestion_date=*"))
    assert Path(clean["reports"]["summary"]).is_file()
    assert set(clean["figures"]) == {
        "missingness_heatmap",
        "lineage_dag",
        "sensor_physical_bands",
    }
    assert all(Path(path).is_file() for path in clean["figures"].values())
    assert (clean_artifacts / "data_docs" / "index.html").is_file()
    spooled = sorted((clean_artifacts / "lineage_spool").rglob("*.json"))
    assert len(spooled) == 2

    version_before_duplicate = DeltaTable(str(table_uri)).version()
    duplicate_summary, duplicate_exit = _run(
        clean_bronze, silver_root, tmp_path / "duplicate_artifacts"
    )
    assert duplicate_exit == 0
    assert duplicate_summary["results"][0]["status"] == "SKIPPED_DUPLICATE"
    assert DeltaTable(str(table_uri)).version() == version_before_duplicate
    assert pl.read_delta(table_uri).height == 200

    boundary_bronze = tmp_path / "boundary_bronze"
    boundary_artifacts = tmp_path / "boundary_artifacts"
    _write_case(boundary_bronze, 200, invalid_rows=1)
    boundary_summary, boundary_exit = _run(
        boundary_bronze, tmp_path / "boundary_silver", boundary_artifacts
    )
    boundary = boundary_summary["results"][0]
    assert boundary_exit == 0
    assert boundary["status"] == "PASS_WITH_REJECTIONS"
    assert boundary["valid_rows"] == 199
    assert boundary["rejected_rows"] == 1
    assert pl.read_delta(tmp_path / "boundary_silver" / "sensor_telemetry").height == 199
    assert pl.read_parquet(boundary["reports"]["quarantine"]).height == 1

    failing_bronze = tmp_path / "failing_bronze"
    _write_case(failing_bronze, 200, invalid_rows=2)
    version_before_fail = DeltaTable(str(table_uri)).version()
    failing_summary, failing_exit = _run(
        failing_bronze, silver_root, tmp_path / "failing_artifacts"
    )
    failing = failing_summary["results"][0]
    assert failing_exit == 2
    assert failing["status"] == "FAIL"
    assert failing["rows_written"] == 0
    assert DeltaTable(str(table_uri)).version() == version_before_fail
    assert pl.read_delta(table_uri).height == 200

    corrupt_bronze = tmp_path / "corrupt_bronze"
    corrupt_path = _write_case(corrupt_bronze, 20)
    corrupt_path.with_suffix(".sha256").write_text("0" * 64, encoding="utf-8")
    corrupt_summary, corrupt_exit = _run(
        corrupt_bronze, tmp_path / "corrupt_silver", tmp_path / "corrupt_artifacts"
    )
    corrupt = corrupt_summary["results"][0]
    assert corrupt_exit == 2
    assert corrupt["status"] == "FAIL"
    assert corrupt["error_category"] == "quality"
    assert "Checksum mismatch" in corrupt["reason"]
    assert not (tmp_path / "corrupt_silver" / "sensor_telemetry").exists()
