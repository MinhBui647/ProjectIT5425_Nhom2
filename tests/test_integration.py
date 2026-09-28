"""End-to-end integration test: Bronze -> Silver -> Time-Travel -> Audit.

Runs entirely on a local Delta table under ``tmp_path`` (no MinIO required),
using the Bronze/Silver temp fixtures from ``conftest.py``.
"""

from datetime import datetime, timezone
from pathlib import Path

import polars as pl

from lakehouse_storage.bronze_writer import write_bronze_batch
from lakehouse_storage.silver_manager import ingest_bronze_file, read_silver_table
from lakehouse_storage.time_travel import (
    diff_versions,
    generate_audit_report,
    trace_batch,
)


def _mes_bronze_df(batch_id: str, start_hour: int = 1) -> pl.DataFrame:
    start = datetime(2026, 1, 15, start_hour, 0, 0, tzinfo=timezone.utc)
    return pl.DataFrame([{
        "batch_id": batch_id,
        "line_id": "LINE_UHT_1",
        "production_start": start.isoformat(),
        "production_end": start.replace(hour=start_hour + 2).isoformat(),
        "sku_id": "UHT_PURE",
        "fat_in": 3.5,
        "protein_in": 3.2,
        "acidity_sh": 6.5,
        "yield_recovery_pct": 95.0,
        "microbiology_status": "PASSED",
        "batch_defect_flag": 0,
        "_source_system": "integration_test",
        "_generated_at": datetime.now(timezone.utc).isoformat(),
    }])


def test_bronze_to_silver_time_travel_audit(bronze_temp_path, silver_temp_path):
    silver_uri = str(silver_temp_path / "mes_lims")

    # Bronze
    bronze_file = write_bronze_batch(
        _mes_bronze_df("BATCH_E2E_01"), "mes_lims", bronze_root=bronze_temp_path
    )

    # Silver
    ingest = ingest_bronze_file(
        bronze_file,
        silver_uri,
        partition_by=["line_id", "ingestion_date"],
        table_name="mes_lims",
        bronze_root=bronze_temp_path,
    )
    assert ingest["rows_ingested"] == 1
    assert len(ingest["sha256"]) == 64

    silver = read_silver_table(silver_uri)
    assert len(silver) == 1
    assert silver["_bronze_file"][0] == str(Path(bronze_file).resolve())
    assert silver["_bronze_sha256"][0] == ingest["sha256"]

    # Traceability (ISO 22000)
    traced = trace_batch(silver_uri, "BATCH_E2E_01")
    assert traced["integrity_verified"] is True
    assert traced["lineage"]["bronze_file"] == str(Path(bronze_file).resolve())

    # Audit report
    report = generate_audit_report(silver_uri, "2026-01-01", "2026-12-31")
    assert len(report) == 1
    assert "_bronze_file" in report.columns
    assert "_bronze_sha256" in report.columns

    # Time travel: append a second batch and diff versions
    second = write_bronze_batch(
        _mes_bronze_df("BATCH_E2E_02", start_hour=5),
        "mes_lims",
        bronze_root=bronze_temp_path,
    )
    ingest_bronze_file(
        second,
        silver_uri,
        partition_by=["line_id", "ingestion_date"],
        table_name="mes_lims",
        bronze_root=bronze_temp_path,
    )

    diff = diff_versions(silver_uri, 0, 1)
    assert diff["counts"]["v1_rows"] == 1
    assert diff["counts"]["v2_rows"] == 2
    assert diff["counts"]["added_count"] == 1
