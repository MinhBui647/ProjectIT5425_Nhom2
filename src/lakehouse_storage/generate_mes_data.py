"""Generate mock MES/LIMS production batch data.

One row per production batch. Output matches BRONZE_MES_LIMS_SCHEMA
(string timestamps, float64).

The data is simulated, but it follows a known mechanism (GROUND_TRUTH below,
documented in docs/ground_truth.md) so that the statistical models can be
checked by recovering the coefficients that were put in.
"""

from datetime import datetime, timedelta, timezone
import argparse
import functools
import json
import math
import os
from pathlib import Path

import numpy as np
import polars as pl

from .config import DEFAULT_LINE_IDS, DEFAULT_SKU_CODES, SILVER_TABLE_URIS, get_storage_options
from .schemas import VALID_SKU_CODES, VALID_MICROBIOLOGY_STATUS


# =============================================================================
# Simulator coefficients. Keep in sync with docs/ground_truth.md.
# =============================================================================

FARMS = ["MOC_CHAU", "BA_VI", "NGHE_AN", "LAM_DONG", "CU_CHI"]

GROUND_TRUTH = {
    # Raw milk: fat ~ N(3.5 + farm offset, 0.1), protein ~ N(3.2, 0.05), acidity ~ N(6.5, 0.2)
    "fat_mean": 3.5,
    "fat_sd": 0.1,
    "farm_fat_offset": {"MOC_CHAU": 0.08, "BA_VI": 0.0, "NGHE_AN": -0.04, "LAM_DONG": 0.04, "CU_CHI": -0.08},
    "protein_mean": 3.2,
    "protein_sd": 0.05,
    "acidity_mean": 6.5,
    "acidity_sd": 0.2,
    # Sensors: every sensor of a batch shares one drift ~ N(0, 0.1); UHT level = 140 + drift
    "drift_sd": 0.1,
    "uht_base": 140.0,
    # Defect: logit p = intercept + uht_coef * (uht_level - 140) + acidity_coef * (acidity - 6.5) / 0.2
    "defect_uht_coef": -8.0,
    "defect_acidity_coef": 1.5,  # per standard deviation of acidity
    # Yield (%) = 95 + 8 (fat - 3.5) + 5 (protein - 3.2) - 0.5 (uht_level - 140)
    #             - 6 * defect + line offset + N(0, 1), capped at 100
    "yield_intercept": 95.0,
    "yield_fat_coef": 8.0,
    "yield_protein_coef": 5.0,
    "yield_uht_coef": -0.5,
    "yield_defect_coef": -6.0,
    "yield_noise_sd": 1.0,
    "line_yield_offset": {"LINE_UHT_1": 0.0, "LINE_UHT_2": -0.8},
    # Flow (L/h): batch level = 5000 + flow_dev, flow_dev ~ N(0, 150)
    "flow_base_lph": 5000.0,
    "flow_batch_sd": 150.0,
    # Power (kW): batch level = 75 + drift + 0.01 * flow_dev + SKU offset
    "power_base_kw": 75.0,
    "power_flow_coef": 0.01,
    "sku_power_offset": {"UHT_PURE": 0.0, "UHT_SWEETENED": 3.0, "UHT_LOWFAT": -2.0},
}


@functools.lru_cache(maxsize=64)
def defect_intercept(defect_rate: float) -> float:
    """Intercept that makes the overall defect rate equal ``defect_rate``.

    The random part of the linear predictor, -8 * drift + 1.5 * z_acidity, is
    normal with sd s = sqrt(0.8^2 + 1.5^2). The intercept a solves
    E[logistic(a + s Z)] = defect_rate (Gauss-Hermite quadrature + bisection).
    For defect_rate = 0.05 this gives about -4.0.
    """
    gt = GROUND_TRUTH
    s = math.hypot(gt["defect_uht_coef"] * gt["drift_sd"], gt["defect_acidity_coef"])
    nodes, weights = np.polynomial.hermite_e.hermegauss(40)  # weight exp(-x^2/2)
    weights = weights / weights.sum()

    def mean_p(a: float) -> float:
        return float(np.sum(weights / (1.0 + np.exp(-(a + s * nodes)))))

    lo, hi = -30.0, 30.0
    for _ in range(80):
        mid = (lo + hi) / 2.0
        lo, hi = (mid, hi) if mean_p(mid) < defect_rate else (lo, mid)
    return (lo + hi) / 2.0


def defect_probability(uht_level: float, acidity: float, defect_rate: float = 0.05) -> float:
    """True probability that a batch is defective (lower UHT or more acidic milk -> riskier)."""
    if defect_rate <= 0.0:
        return 0.0
    if defect_rate >= 1.0:
        return 1.0
    gt = GROUND_TRUTH
    logit = (
        defect_intercept(defect_rate)
        + gt["defect_uht_coef"] * (uht_level - gt["uht_base"])
        + gt["defect_acidity_coef"] * (acidity - gt["acidity_mean"]) / gt["acidity_sd"]
    )
    return 1.0 / (1.0 + math.exp(-logit))


def _line_seed_offset(line_id: str) -> int:
    """Keep batch seeds of different lines apart (seed * 4 + i stays far below 1e6)."""
    if line_id in DEFAULT_LINE_IDS:
        return DEFAULT_LINE_IDS.index(line_id) * 1_000_000
    return (sum(line_id.encode()) % 900 + 100) * 1_000_000


def generate_mes_batch(
    date: datetime,
    line_id: str = "LINE_UHT_1",
    sku_id: str = "UHT_PURE",
    batch_id: str | None = None,
    production_start: datetime | None = None,
    production_end: datetime | None = None,
    seed: int | None = None,
    include_defect: bool | None = False,
    farm_id: str | None = None,
    uht_level: float | None = None,
    defect_rate: float = 0.05,
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
        include_defect: True/False forces the defect flag. None draws it from
            the defect mechanism (needs ``uht_level``; see GROUND_TRUTH).
        farm_id: Supplying farm; shifts the mean fat content.
        uht_level: Batch UHT level from the sensor stream (140 + drift).
        defect_rate: Target overall defect rate when include_defect is None.

    Returns:
        DataFrame matching the Bronze mes_lims schema.
    """
    rng = np.random.default_rng(seed)
    gt = GROUND_TRUTH

    # Default: 08:00 ICT = 01:00 UTC (ICT = UTC+7)
    if production_start is None:
        production_start = datetime(date.year, date.month, date.day, 1, 0, 0, tzinfo=timezone.utc)
    if production_end is None:
        production_end = production_start + timedelta(hours=2)

    batch_id = batch_id or f"BATCH_{date.strftime('%Y%m%d')}_{line_id}"
    if uht_level is None:
        uht_level = gt["uht_base"]

    # Raw milk composition (farm decides the mean fat)
    fat = rng.normal(gt["fat_mean"] + gt["farm_fat_offset"].get(farm_id, 0.0), gt["fat_sd"])
    protein = rng.normal(gt["protein_mean"], gt["protein_sd"])
    acidity = rng.normal(gt["acidity_mean"], gt["acidity_sd"])

    # Defect flag: forced by the caller, or drawn from the mechanism
    if include_defect is None:
        include_defect = bool(rng.random() < defect_probability(uht_level, acidity, defect_rate))
    defect_flag = 1 if include_defect else 0

    # Yield depends on milk, UHT level, defect and line
    yield_pct = (
        gt["yield_intercept"]
        + gt["yield_fat_coef"] * (fat - gt["fat_mean"])
        + gt["yield_protein_coef"] * (protein - gt["protein_mean"])
        + gt["yield_uht_coef"] * (uht_level - gt["uht_base"])
        + gt["yield_defect_coef"] * defect_flag
        + gt["line_yield_offset"].get(line_id, 0.0)
        + rng.normal(0.0, gt["yield_noise_sd"])
    )
    yield_pct = min(yield_pct, 100.0)

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
    # 4 batches per day, 2 hours each
    batches_per_day = 4
    hours_per_batch = 2
    samples_per_hour = 3600  # 1 sample/second

    mes_rows = []
    sensor_rows = []
    batch_farm_map = {}

    for i in range(batches_per_day):
        # Callers pass seed = base + day, so seed * 4 + i is unique for every (day, batch).
        # The line offset keeps LINE_UHT_2 apart from LINE_UHT_1 on the same day.
        batch_seed = seed * 4 + i + _line_seed_offset(line_id) if seed is not None else None
        # Separate stream for sensors so MES values are not copies of the sensor drift.
        sensor_seed = batch_seed + 10_000_000 if batch_seed is not None else None
        batch_rng = np.random.default_rng(batch_seed)

        # Batch times: 08:00-14:00 ICT = 01:00-07:00 UTC
        batch_start = datetime(date.year, date.month, date.day, 1 + i * 2, 0, 0, tzinfo=timezone.utc)
        batch_end = batch_start + timedelta(hours=hours_per_batch)

        # Batch code by date and order
        batch_id = f"BATCH_{date.strftime('%Y%m%d')}_{i+1:02d}_{line_id}"

        # Random SKU and supplying farm (both known before the batch starts)
        sku_id = str(batch_rng.choice(DEFAULT_SKU_CODES))
        farm_id = str(batch_rng.choice(FARMS))

        # 1) Sensors first: the batch drift sets the UHT level used by the defect and yield mechanism
        n_samples = hours_per_batch * samples_per_hour
        sensor_batch, drift = _generate_sensor_batch_for_mes(
            n_samples=n_samples,
            line_id=line_id,
            batch_id=batch_id,
            start_time=batch_start,
            seed=sensor_seed,
            sku_id=sku_id,
            return_drift=True,
        )
        sensor_rows.append(sensor_batch)

        # 2) MES row: defect and yield are drawn from the mechanism in GROUND_TRUTH
        mes_row = generate_mes_batch(
            date=date,
            line_id=line_id,
            sku_id=sku_id,
            batch_id=batch_id,
            production_start=batch_start,
            production_end=batch_end,
            seed=batch_seed + 20_000_000 if batch_seed is not None else None,  # own stream, not reused
            include_defect=None,
            farm_id=farm_id,
            uht_level=GROUND_TRUTH["uht_base"] + drift,
            defect_rate=defect_rate,
        )
        mes_rows.append(mes_row)

        # Map batch to farm (simulated)
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
    sku_id: str | None = None,
    return_drift: bool = False,
) -> pl.DataFrame | tuple[pl.DataFrame, float]:
    """Generate sensor data for one MES batch.

    Sensors share a latent batch drift; sample-level correlations can be
    weak relative to the independent noise. Flow has its own level; power follows
    flow and product (GROUND_TRUTH). With return_drift=True the batch drift
    is returned too, so the MES mechanism can use the UHT level.
    """
    gt = GROUND_TRUTH
    rng = np.random.default_rng(seed)
    timestamps = [start_time + timedelta(seconds=i) for i in range(n_samples)]

    # Per-batch levels
    drift = float(rng.normal(0, gt["drift_sd"]))
    flow_dev = float(rng.normal(0, gt["flow_batch_sd"]))
    power_level = (
        gt["power_base_kw"] + drift
        + gt["power_flow_coef"] * flow_dev
        + gt["sku_power_offset"].get(sku_id, 0.0)
    )

    df = pl.DataFrame({
        "timestamp": [ts.isoformat() for ts in timestamps],
        "line_id": [line_id] * n_samples,
        "batch_id": [batch_id] * n_samples,
        "preheat_temp": rng.normal(80.0 + drift, 0.5, n_samples),
        "uht_temp": rng.normal(gt["uht_base"] + drift, 0.8, n_samples),
        "homo_press_stage1": rng.normal(180.0 + drift, 3.0, n_samples),
        "homo_press_stage2": rng.normal(40.0 + drift, 1.0, n_samples),
        "flow_rate": rng.normal(gt["flow_base_lph"] + flow_dev + drift * 10, 80.0, n_samples),
        "conductivity": rng.normal(4.8 + drift * 0.1, 0.1, n_samples),
        "power_kw": rng.normal(power_level, 2.0, n_samples),
        "_source_system": ["mock_sensor_stream"] * n_samples,
        "_generated_at": [datetime.now(timezone.utc).isoformat()] * n_samples,
    })
    return (df, drift) if return_drift else df


def write_mes_batch(
    df: pl.DataFrame,
    table_uri: str | None = None,
    bronze_root: str | Path | None = None,
) -> int:
    """Write MES batch to Silver through the Bronze layer.

    Low-level compatibility helper: includes lineage but does not run P3 or
    ensure Gold eligibility. Use the CLI or governed entry point for Gold.
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
    import tempfile

    from .bronze_writer import write_bronze_batch
    from .config import BRONZE_LOCAL_PATH

    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--days", type=int, default=1, help="Number of days")
    parser.add_argument("--date", type=str, default=None, help="Date (YYYY-MM-DD)")
    parser.add_argument("--seed", type=int, default=5425)
    parser.add_argument(
        "--line-id", default=",".join(DEFAULT_LINE_IDS),
        help="One line or a comma-separated list (default: all lines in config)",
    )
    parser.add_argument("--defect-rate", type=float, default=0.05)
    parser.add_argument("--output-dir", default="data/synthetic", help="Output directory")
    write_mode = parser.add_mutually_exclusive_group()
    write_mode.add_argument("--write-delta", action="store_true", help="Write through P3 to Silver")
    write_mode.add_argument("--write-bronze", action="store_true", help="Write only to Bronze")
    parser.add_argument("--bronze-root", default=os.getenv("STORAGE_GENERATOR_BRONZE_ROOT"))
    parser.add_argument("--silver-root", default=os.getenv("STORAGE_GENERATOR_SILVER_ROOT"))
    parser.add_argument(
        "--reports-root",
        default=os.getenv("STORAGE_GENERATOR_REPORTS_ROOT", "reports/storage/generator"),
    )
    args = parser.parse_args()

    if args.days < 1:
        parser.error("--days must be at least 1")
    if args.seed is not None and args.seed < 0:
        parser.error("--seed must be non-negative")
    if not 0.0 <= args.defect_rate <= 1.0:
        parser.error("--defect-rate must be between 0 and 1")
    if args.write_delta and not args.silver_root:
        parser.error("--silver-root (or STORAGE_GENERATOR_SILVER_ROOT) is required for governed output")

    line_ids = [value.strip() for value in args.line_id.split(",") if value.strip()]
    if not line_ids or any(line not in DEFAULT_LINE_IDS for line in line_ids):
        parser.error("--line-id must contain supported lines: " + ", ".join(DEFAULT_LINE_IDS))
    if len(line_ids) != len(set(line_ids)):
        parser.error("--line-id must not repeat a line")

    if args.date:
        try:
            start_date = datetime.strptime(args.date, "%Y-%m-%d")
        except ValueError:
            parser.error("--date must be a valid date in YYYY-MM-DD format")
    else:
        start_date = datetime.now(timezone.utc)

    bronze_root = args.bronze_root or BRONZE_LOCAL_PATH
    output_dir = Path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    map_file = output_dir / "batch_farm_map.json"
    batch_farm_maps = {}
    if map_file.exists():
        try:
            previous_maps = json.loads(map_file.read_text(encoding="utf-8"))
            if not isinstance(previous_maps, list) or any(
                not isinstance(day, dict) for day in previous_maps
            ):
                raise ValueError("expected a list of batch mappings")
            for day in previous_maps:
                for batch, info in day.items():
                    if not isinstance(info, dict) or not isinstance(info.get("farm_id"), str):
                        raise ValueError("each batch mapping must contain a string farm_id")
                    if batch in batch_farm_maps and batch_farm_maps[batch] != info:
                        raise ValueError(f"conflicting existing mapping for {batch}")
                    batch_farm_maps[batch] = info
        except (OSError, ValueError) as exc:
            parser.error(f"Cannot read {map_file}: {exc}")

    def save_batch_farm_map():
        # Keep the list-of-dicts format expected by Gold, replacing atomically.
        descriptor, temporary_name = tempfile.mkstemp(
            prefix=".batch_farm_map-", suffix=".tmp", dir=output_dir,
        )
        try:
            with os.fdopen(descriptor, "w", encoding="utf-8") as stream:
                json.dump([batch_farm_maps], stream, indent=2, default=str)
            os.replace(temporary_name, map_file)
        finally:
            Path(temporary_name).unlink(missing_ok=True)

    all_sensor_rows = []
    all_mes_rows = []

    for day_offset in range(args.days):
        day_date = start_date + timedelta(days=day_offset)
        day_seed = args.seed + day_offset if args.seed is not None else None

        for line_id in line_ids:
            sensor_df, mes_df, batch_farm_map = generate_daily_production(
                date=day_date,
                line_id=line_id,
                seed=day_seed,
                defect_rate=args.defect_rate,
            )
            for batch, info in batch_farm_map.items():
                if batch in batch_farm_maps and batch_farm_maps[batch] != info:
                    parser.error(f"Conflicting farm mapping for {batch}; use a separate --output-dir")
            batch_farm_maps.update(batch_farm_map)

            if args.write_bronze or args.write_delta:
                for table, frame in (("sensor_telemetry", sensor_df), ("mes_lims", mes_df)):
                    path = write_bronze_batch(frame, table, bronze_root=bronze_root)
                    # Persist lineage before P3 can reject the published data.
                    save_batch_farm_map()
                    if args.write_delta:
                        from .governance_adapter import ingest_governed_bronze_file

                        result = ingest_governed_bronze_file(
                            path, table, bronze_root=bronze_root,
                            silver_root=args.silver_root, reports_root=args.reports_root,
                        )
                        print(json.dumps(result, default=str), flush=True)
                        if result["status"] not in {"PASS", "PASS_WITH_REJECTIONS", "SKIPPED_DUPLICATE"}:
                            return 1
                    else:
                        print(f"Bronze: {path} ({len(frame)} rows)")
            else:
                all_sensor_rows.append(sensor_df)
                all_mes_rows.append(mes_df)

            print(f"Day {day_date.date()} {line_id}: sensor={len(sensor_df)} rows, mes={len(mes_df)} rows")

    if not (args.write_bronze or args.write_delta):
        all_sensor = pl.concat(all_sensor_rows)
        sensor_file = output_dir / f"sensors_{start_date.strftime('%Y-%m')}.parquet"
        all_sensor.write_parquet(sensor_file)
        print(f"Saved: {sensor_file} ({len(all_sensor)} rows)")

        all_mes = pl.concat(all_mes_rows)
        mes_file = output_dir / f"mes_{start_date.strftime('%Y-%m')}.parquet"
        all_mes.write_parquet(mes_file)
        print(f"Saved: {mes_file} ({len(all_mes)} rows)")
        save_batch_farm_map()

    print(f"Saved: {map_file}")
    return 0


def _write_sensor_to_delta(
    df: pl.DataFrame,
    table_uri: str | None = None,
    bronze_root: str | Path | None = None,
) -> int:
    """Low-level compatibility helper; does not run P3 or ensure Gold eligibility."""
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
    raise SystemExit(main())
