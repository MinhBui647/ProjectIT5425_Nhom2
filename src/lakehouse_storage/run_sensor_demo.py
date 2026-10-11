"""Run one isolated Sensor -> Bronze -> Silver demonstration."""

from __future__ import annotations

import argparse
from datetime import datetime, timezone
import json
from pathlib import Path
import re

from deltalake import DeltaTable
import polars as pl

from .bronze_writer import verify_checksum, write_bronze_batch
from .config import BRONZE_LOCAL_PATH, MINIO_BUCKET, get_storage_options
from .mock_sensor_stream import generate_sensor_batch
from .silver_manager import ingest_bronze_file, read_silver_table
from .utils import sha256_file


DEMO_PARTITIONS = ["line_id", "ingestion_date"]
FLOAT_COLUMNS = [
    "preheat_temp",
    "uht_temp",
    "homo_press_stage1",
    "homo_press_stage2",
    "flow_rate",
    "conductivity",
    "power_kw",
]
RUN_ID_PATTERN = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._-]*$")


def parse_utc_datetime(value: str) -> datetime:
    """Parse an ISO-8601 timestamp and normalize it to UTC."""
    parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    if parsed.tzinfo is None:
        raise ValueError("start_time must include a timezone offset")
    return parsed.astimezone(timezone.utc)


def build_demo_silver_uri(run_id: str, bucket: str = MINIO_BUCKET) -> str:
    """Return an isolated Silver URI for one demo run."""
    if not RUN_ID_PATTERN.fullmatch(run_id):
        raise ValueError(
            "run_id must start with an alphanumeric character and contain only "
            "letters, numbers, dot, underscore, or hyphen"
        )
    return f"s3://{bucket}/02_silver_curated/_demo/sensor_telemetry/{run_id}"


def run_sensor_demo(
    *,
    run_id: str,
    samples: int,
    line_id: str,
    batch_id: str,
    seed: int,
    start_time: datetime,
    bronze_root: str | Path,
    silver_table_uri: str,
) -> dict:
    """Generate Sensor data, persist Bronze, ingest Silver, and verify output.

    The Silver target must not already be a Delta table. This makes retries with
    the same run id fail explicitly instead of silently appending duplicates.
    """
    if not RUN_ID_PATTERN.fullmatch(run_id):
        raise ValueError(f"Invalid run_id: {run_id!r}")

    storage_options = get_storage_options()
    if DeltaTable.is_deltatable(silver_table_uri, storage_options=storage_options):
        raise FileExistsError(
            f"Demo Silver table already exists for run_id={run_id}: "
            f"{silver_table_uri}"
        )

    bronze_root = Path(bronze_root).resolve()
    raw_df = generate_sensor_batch(
        n_samples=samples,
        line_id=line_id,
        batch_id=batch_id,
        start_time=start_time,
        seed=seed,
    )
    bronze_path = Path(
        write_bronze_batch(
            raw_df,
            "sensor_telemetry",
            bronze_root=bronze_root,
        )
    ).resolve()

    if not verify_checksum(bronze_path):
        raise RuntimeError(f"Bronze checksum verification failed: {bronze_path}")
    bronze_sha256 = sha256_file(bronze_path)

    ingest_result = ingest_bronze_file(
        bronze_path,
        silver_table_uri,
        partition_by=DEMO_PARTITIONS,
        table_name="sensor_telemetry",
        bronze_root=bronze_root,
        enforce_checksum=True,
    )
    silver_df = read_silver_table(
        silver_table_uri,
        line_id=line_id,
        date_from=start_time.date(),
        date_to=start_time.date(),
    )
    delta_table = DeltaTable(silver_table_uri, storage_options=storage_options)

    _verify_demo_result(
        silver_df=silver_df,
        expected_rows=samples,
        expected_line_id=line_id,
        expected_batch_id=batch_id,
        expected_bronze_path=bronze_path,
        expected_sha256=bronze_sha256,
    )

    return {
        "status": "PASS",
        "run_id": run_id,
        "samples": samples,
        "line_id": line_id,
        "batch_id": batch_id,
        "seed": seed,
        "start_time_utc": start_time.astimezone(timezone.utc).isoformat(),
        "bronze_path": str(bronze_path),
        "bronze_sha256": bronze_sha256,
        "silver_table_uri": silver_table_uri,
        "delta_version": delta_table.version(),
        "silver_rows": silver_df.height,
        "silver_columns": silver_df.width,
        "delta_data_files": len(delta_table.file_uris()),
        "rows_ingested": ingest_result["rows_ingested"],
        "partition_by": DEMO_PARTITIONS,
    }


def _verify_demo_result(
    *,
    silver_df: pl.DataFrame,
    expected_rows: int,
    expected_line_id: str,
    expected_batch_id: str,
    expected_bronze_path: Path,
    expected_sha256: str,
) -> None:
    """Raise when the Silver result differs from the demo contract."""
    if silver_df.height != expected_rows:
        raise RuntimeError(
            f"Silver row count mismatch: expected {expected_rows}, "
            f"got {silver_df.height}"
        )
    if silver_df.schema["timestamp"].time_zone != "UTC":
        raise RuntimeError("Silver timestamp is not UTC")
    if any(silver_df.schema[column] != pl.Float32 for column in FLOAT_COLUMNS):
        raise RuntimeError("Silver measurement columns are not all Float32")
    if silver_df["line_id"].unique().to_list() != [expected_line_id]:
        raise RuntimeError("Silver line_id differs from the requested line")
    if silver_df["batch_id"].unique().to_list() != [expected_batch_id]:
        raise RuntimeError("Silver batch_id differs from the requested batch")
    if silver_df["_bronze_file"].unique().to_list() != [str(expected_bronze_path)]:
        raise RuntimeError("Silver lineage does not point to the generated Bronze file")
    if silver_df["_bronze_sha256"].unique().to_list() != [expected_sha256]:
        raise RuntimeError("Silver lineage checksum differs from Bronze")


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--run-id", required=True)
    parser.add_argument("--samples", type=int, default=60)
    parser.add_argument("--line-id", default="LINE_UHT_1")
    parser.add_argument("--batch-id", default="BATCH_DEMO_001")
    parser.add_argument("--seed", type=int, default=42)
    parser.add_argument(
        "--start-time",
        default=datetime.now(timezone.utc).isoformat(),
        help="ISO-8601 timestamp with timezone; defaults to now in UTC",
    )
    parser.add_argument("--bronze-root", default=BRONZE_LOCAL_PATH)
    parser.add_argument("--silver-table-uri", default=None)
    args = parser.parse_args()

    silver_table_uri = args.silver_table_uri or build_demo_silver_uri(args.run_id)
    result = run_sensor_demo(
        run_id=args.run_id,
        samples=args.samples,
        line_id=args.line_id,
        batch_id=args.batch_id,
        seed=args.seed,
        start_time=parse_utc_datetime(args.start_time),
        bronze_root=args.bronze_root,
        silver_table_uri=silver_table_uri,
    )
    print(json.dumps(result, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
