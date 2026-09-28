"""Generate mock MES/LIMS production batch data.

One row per production batch. Output matches BRONZE_MES_LIMS_SCHEMA
(string timestamps, float64).
"""

from datetime import datetime, timedelta, timezone
import argparse
import json
from pathlib import Path

import numpy as np
import polars as pl

from .config import DEFAULT_SKU_CODES, SILVER_TABLE_URIS, get_storage_options
from .schemas import VALID_SKU_CODES, VALID_MICROBIOLOGY_STATUS


def generate_mes_batch(
    date: datetime,
    line_id: str = "LINE_UHT_1",
    sku_id: str = "UHT_PURE",
    batch_id: str | None = None,
    production_start: datetime | None = None,
    production_end: datetime | None = None,
    seed: int | None = None,
    include_defect: bool = False,
) -> pl.DataFrame:
    """Generate one MES/LIMS batch.

    Args:
        date: Production date.
        line_id: Production line code.
        sku_id: SKU code.
        batch_id: Batch code (auto-generated if None).
        production_start: Start time (default: 08:00 ICT).
        production_end: End time (default: +2h).
        seed: Random seed for reproducibility.
        include_defect: Whether the batch is defective.

    Returns:
        DataFrame matching the Bronze mes_lims schema.
    """
    rng = np.random.default_rng(seed)

    # Default: 08:00 ICT = 01:00 UTC (ICT = UTC+7)
    if production_start is None:
        production_start = datetime(date.year, date.month, date.day, 1, 0, 0, tzinfo=timezone.utc)
    if production_end is None:
        production_end = production_start + timedelta(hours=2)

    batch_id = batch_id or f"BATCH_{date.strftime('%Y%m%d')}_{line_id}"

    # Defect flag
    defect_flag = 1 if include_defect else 0

    # Generate quality metrics
    fat = rng.normal(3.5, 0.1)
    protein = rng.normal(3.2, 0.05)
    acidity = rng.normal(6.5, 0.2)
    yield_pct = rng.normal(95.0, 2.0) if defect_flag == 0 else rng.normal(88.0, 3.0)

    # Microbiology status
    if defect_flag == 1:
        micro_status = "FAILED"
    else:
        micro_status = rng.choice(["PASSED", "PASSED", "PASSED", "PENDING"])

    return pl.DataFrame([{
        "batch_id": batch_id,
        "line_id": line_id,
        "production_start": production_start.isoformat(),
        "production_end": production_end.isoformat(),
        "sku_id": sku_id,
        "fat_in": fat,
        "protein_in": protein,
        "acidity_sh": acidity,
        "yield_recovery_pct": yield_pct,
        "microbiology_status": micro_status,
        "batch_defect_flag": defect_flag,
        "_source_system": "mock_mes_generator",
        "_generated_at": datetime.now(timezone.utc).isoformat(),
    }])


def generate_daily_production(
    date: datetime,
    line_id: str = "LINE_UHT_1",
    seed: int | None = None,
    defect_rate: float = 0.05,
) -> tuple[pl.DataFrame, pl.DataFrame, dict]:
    """Generate one production day (8 hours, 4 batches).

    Args:
        date: Production date.
        line_id: Production line code.
        seed: Random seed for reproducibility.
        defect_rate: Fraction of defective batches.

    Returns:
        Tuple of (sensor_df, mes_df, batch_farm_map):
        - sensor_df: sensor DataFrame (8h x 3600 samples/h = 28800 rows).
        - mes_df: MES DataFrame (4 batches).
        - batch_farm_map: dict mapping batch_id to farm_id.
    """
    rng = np.random.default_rng(seed)

    # 4 batches per day, 2 hours each
    batches_per_day = 4
    hours_per_batch = 2
    samples_per_hour = 3600  # 1 sample/second

    mes_rows = []
    sensor_rows = []
    batch_farm_map = {}

    farms = ["MOC_CHAU", "BA_VI", "NGHE_AN", "LAM_DONG", "CU_CHI"]

    for i in range(batches_per_day):
        batch_seed = (seed or 0) + i if seed is not None else None
        batch_rng = np.random.default_rng(batch_seed)

        # Batch times: 08:00-14:00 ICT = 01:00-07:00 UTC
        batch_start = datetime(date.year, date.month, date.day, 1 + i * 2, 0, 0, tzinfo=timezone.utc)
        batch_end = batch_start + timedelta(hours=hours_per_batch)

        # Batch code by date and order
        batch_id = f"BATCH_{date.strftime('%Y%m%d')}_{i+1:02d}_{line_id}"

        # Random SKU
        sku_id = batch_rng.choice(DEFAULT_SKU_CODES)

        # Decide defect flag
        include_defect = batch_rng.random() < defect_rate

        # Build MES row
        mes_row = generate_mes_batch(
            date=date,
            line_id=line_id,
            sku_id=sku_id,
            batch_id=batch_id,
            production_start=batch_start,
            production_end=batch_end,
            seed=batch_seed,
            include_defect=include_defect,
        )
        mes_rows.append(mes_row)

        # Generate sensor data for this batch
        n_samples = hours_per_batch * samples_per_hour
        sensor_batch = _generate_sensor_batch_for_mes(
            n_samples=n_samples,
            line_id=line_id,
            batch_id=batch_id,
            start_time=batch_start,
            seed=batch_seed,
        )
        sensor_rows.append(sensor_batch)

        # Map batch to farm (simulated)
        farm_id = batch_rng.choice(farms)
        batch_farm_map[batch_id] = {
            "farm_id": farm_id,
            "line_id": line_id,
            "production_date": date.date().isoformat(),
        }

    mes_df = pl.concat(mes_rows)
    sensor_df = pl.concat(sensor_rows)

    return sensor_df, mes_df, batch_farm_map


def _generate_sensor_batch_for_mes(
    n_samples: int,
    line_id: str,
    batch_id: str,
    start_time: datetime,
    seed: int | None = None,
) -> pl.DataFrame:
    """Generate sensor data for one MES batch."""
    rng = np.random.default_rng(seed)
    timestamps = [start_time + timedelta(seconds=i) for i in range(n_samples)]

    # Add per-batch drift and noise
    drift = rng.normal(0, 0.1)

    return pl.DataFrame({
        "timestamp": [ts.isoformat() for ts in timestamps],
        "line_id": [line_id] * n_samples,
        "batch_id": [batch_id] * n_samples,
        "preheat_temp": rng.normal(80.0 + drift, 0.5, n_samples),
        "uht_temp": rng.normal(140.0 + drift, 0.8, n_samples),
        "homo_press_stage1": rng.normal(180.0 + drift, 3.0, n_samples),
        "homo_press_stage2": rng.normal(40.0 + drift, 1.0, n_samples),
        "flow_rate": rng.normal(5000.0 + drift * 10, 80.0, n_samples),
        "conductivity": rng.normal(4.8 + drift * 0.1, 0.1, n_samples),
        "power_kw": rng.normal(75.0 + drift, 2.0, n_samples),
        "_source_system": ["mock_sensor_stream"] * n_samples,
        "_generated_at": [datetime.now(timezone.utc).isoformat()] * n_samples,
    })


def write_mes_batch(
    df: pl.DataFrame,
    table_uri: str | None = None,
    bronze_root: str | Path | None = None,
) -> int:
    """Write MES batch to Silver through the Bronze layer.

    The raw batch is persisted to Bronze (Parquet + SHA-256 sidecar) first,
    then ingested into Silver so lineage columns are always populated.
    """
    from deltalake import DeltaTable

    from .bronze_writer import write_bronze_batch
    from .silver_manager import ingest_bronze_file

    if table_uri is None:
        table_uri = SILVER_TABLE_URIS.get("mes_lims", "data/02_silver_curated/mes_lims")

    bronze_path = write_bronze_batch(df, "mes_lims", bronze_root=bronze_root)
    ingest_bronze_file(
        bronze_path,
        table_uri,
        partition_by=["line_id", "ingestion_date"],
        table_name="mes_lims",
        bronze_root=bronze_root,
    )

    storage_opts = get_storage_options()
    return DeltaTable(table_uri, storage_options=storage_opts).version()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--days", type=int, default=1, help="Number of days")
    parser.add_argument("--date", type=str, default=None, help="Date (YYYY-MM-DD)")
    parser.add_argument("--seed", type=int, default=5425)
    parser.add_argument("--line-id", default="LINE_UHT_1")
    parser.add_argument("--defect-rate", type=float, default=0.05)
    parser.add_argument("--output-dir", default="data/synthetic", help="Output directory")
    parser.add_argument("--write-delta", action="store_true", help="Write to Delta table")
    args = parser.parse_args()

    # Parse date
    if args.date:
        start_date = datetime.strptime(args.date, "%Y-%m-%d")
    else:
        start_date = datetime.now(timezone.utc)

    output_dir = Path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    all_sensor_rows = []
    all_mes_rows = []
    all_batch_farm_maps = []

    for day_offset in range(args.days):
        day_date = start_date + timedelta(days=day_offset)
        day_seed = args.seed + day_offset if args.seed else None

        sensor_df, mes_df, batch_farm_map = generate_daily_production(
            date=day_date,
            line_id=args.line_id,
            seed=day_seed,
            defect_rate=args.defect_rate,
        )

        if args.write_delta:
            sensor_version = _write_sensor_to_delta(sensor_df)
            mes_version = write_mes_batch(mes_df)
            print(f"Day {day_date.date()}: sensor v{sensor_version}, mes v{mes_version}")
        else:
            all_sensor_rows.append(sensor_df)
            all_mes_rows.append(mes_df)
            all_batch_farm_maps.append(batch_farm_map)
            print(f"Day {day_date.date()}: sensor={len(sensor_df)} rows, mes={len(mes_df)} rows")

    if not args.write_delta:
        # Save to parquet
        if all_sensor_rows:
            all_sensor = pl.concat(all_sensor_rows)
            sensor_file = output_dir / f"sensors_{start_date.strftime('%Y-%m')}.parquet"
            all_sensor.write_parquet(sensor_file)
            print(f"Saved: {sensor_file} ({len(all_sensor)} rows)")

        if all_mes_rows:
            all_mes = pl.concat(all_mes_rows)
            mes_file = output_dir / f"mes_{start_date.strftime('%Y-%m')}.parquet"
            all_mes.write_parquet(mes_file)
            print(f"Saved: {mes_file} ({len(all_mes)} rows)")

            map_file = output_dir / "batch_farm_map.json"
            with open(map_file, "w") as f:
                json.dump(all_batch_farm_maps, f, indent=2, default=str)
            print(f"Saved: {map_file}")


def _write_sensor_to_delta(
    df: pl.DataFrame,
    table_uri: str | None = None,
    bronze_root: str | Path | None = None,
) -> int:
    """Write sensor data to Silver through the Bronze layer."""
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


if __name__ == "__main__":
    main()
