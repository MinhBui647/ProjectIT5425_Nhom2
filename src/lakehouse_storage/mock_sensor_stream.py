"""Generate mock IoT sensor telemetry data.

One row per sample (1 sample/second). Output matches
BRONZE_SENSOR_TELEMETRY_SCHEMA (string timestamps, float64).
"""

from datetime import datetime, timedelta, timezone
from pathlib import Path
import argparse
import time

import numpy as np
import polars as pl

from .config import SILVER_TABLE_URIS, get_storage_options


def generate_sensor_batch(
    n_samples: int,
    line_id: str = "LINE_UHT_1",
    batch_id: str | None = None,
    start_time: datetime | None = None,
    seed: int | None = None,
) -> pl.DataFrame:
    """Generate a sensor telemetry batch.

    Args:
        n_samples: Number of samples (1 sample = 1 second).
        line_id: Production line code.
        batch_id: Batch code (null if unassigned).
        start_time: Start time (default: now UTC).
        seed: Random seed for reproducibility.

    Returns:
        DataFrame matching the Bronze sensor_telemetry schema.
    """
    if n_samples <= 0:
        raise ValueError("n_samples must be > 0")

    start_time = start_time or datetime.now(timezone.utc)
    if start_time.tzinfo is None:
        start_time = start_time.replace(tzinfo=timezone.utc)

    rng = np.random.default_rng(seed)
    timestamps = [start_time + timedelta(seconds=i) for i in range(n_samples)]

    return pl.DataFrame({
        "timestamp": [ts.isoformat() for ts in timestamps],
        "line_id": [line_id] * n_samples,
        "batch_id": [batch_id] * n_samples if batch_id else [None] * n_samples,
        "preheat_temp": rng.normal(80.0, 0.5, n_samples),
        "uht_temp": rng.normal(140.0, 0.8, n_samples),
        "homo_press_stage1": rng.normal(180.0, 3.0, n_samples),
        "homo_press_stage2": rng.normal(40.0, 1.0, n_samples),
        "flow_rate": rng.normal(5000.0, 80.0, n_samples),
        "conductivity": rng.normal(4.8, 0.1, n_samples),
        "power_kw": rng.normal(75.0, 2.0, n_samples),
        "_source_system": ["mock_sensor_stream"] * n_samples,
        "_generated_at": [datetime.now(timezone.utc).isoformat()] * n_samples,
    })


def write_sensor_batch(
    df: pl.DataFrame,
    table_uri: str | None = None,
    bronze_root: str | Path | None = None,
) -> int:
    """Write sensor batch to Silver through the Bronze layer.

    The raw batch is persisted to Bronze (Parquet + SHA-256 sidecar) first,
    then ingested into Silver so lineage columns are always populated.
    """
    from deltalake import DeltaTable

    from .bronze_writer import write_bronze_batch
    from .silver_manager import ingest_bronze_file

    if table_uri is None:
        table_uri = SILVER_TABLE_URIS.get("sensor_telemetry", "data/02_silver_curated/sensor_telemetry")

    bronze_path = write_bronze_batch(df, "sensor_telemetry", bronze_root=bronze_root)
    ingest_bronze_file(
        bronze_path,
        table_uri,
        partition_by=["line_id", "ingestion_date"],
        table_name="sensor_telemetry",
        bronze_root=bronze_root,
    )

    storage_opts = get_storage_options()
    return DeltaTable(table_uri, storage_options=storage_opts).version()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--batches", type=int, default=3, help="Number of batches (0=forever)")
    parser.add_argument("--samples", type=int, default=60, help="Samples per batch")
    parser.add_argument("--interval", type=float, default=1.0, help="Seconds between batches")
    parser.add_argument("--line-id", default="LINE_UHT_1")
    parser.add_argument("--batch-id", default=None)
    parser.add_argument("--seed", type=int, default=None)
    args = parser.parse_args()

    start = datetime.now(timezone.utc)
    index = 0

    try:
        while args.batches == 0 or index < args.batches:
            df = generate_sensor_batch(
                n_samples=args.samples,
                line_id=args.line_id,
                batch_id=args.batch_id,
                start_time=start,
                seed=args.seed,
            )
            version = write_sensor_batch(df)
            print(f"batch={index} rows={len(df)} version={version}", flush=True)
            start += timedelta(seconds=args.samples)
            index += 1
            if args.batches == 0 or index < args.batches:
                time.sleep(args.interval)
    except KeyboardInterrupt:
        print("Stopped.")


if __name__ == "__main__":
    main()
