"""Verify a generated MES/sensor Bronze dataset without writing Silver or Gold."""

import argparse
import json
import platform
import subprocess
from datetime import date, datetime, timedelta, timezone
from pathlib import Path

import numpy as np
import polars as pl
import pyarrow as pa

from lakehouse_storage.bronze_writer import verify_checksum
from lakehouse_storage.config import DEFAULT_LINE_IDS
from lakehouse_storage.generate_mes_data import FARMS
from lakehouse_storage.schemas import BRONZE_MES_LIMS_SCHEMA, BRONZE_SENSOR_TELEMETRY_SCHEMA
from lakehouse_storage.utils import sha256_file


def require(condition, message):
    if not condition:
        raise ValueError(message)


def read_verified(path, arrow_schema):
    require(verify_checksum(path), f"checksum mismatch or missing sidecar: {path}")
    frame = pl.read_parquet(path)
    expected = pl.from_arrow(pa.Table.from_batches([], schema=arrow_schema)).schema
    for name, dtype in expected.items():
        require(frame.schema.get(name) == dtype, f"schema mismatch for {name}: {path}")
        require(frame[name].null_count() == 0, f"null value in generated column {name}: {path}")
        if dtype in (pl.Float32, pl.Float64):
            require(frame[name].is_finite().all(), f"non-finite value in {name}: {path}")
    return frame


def verify_dataset(bronze_root, mapping_file, *, start, days=90, lines=DEFAULT_LINE_IDS, seed=5425):
    """Read each file once; retain only MES rows and sensor summaries by batch.

    ``seed`` is declared provenance, not a value recoverable from Parquet.
    PASS covers this synthetic Bronze dataset, not a P3 quality decision.
    """
    require(days > 0, "days must be positive")
    require(bool(lines) and len(set(lines)) == len(lines), "lines must be nonempty and distinct")
    require(set(lines) <= set(DEFAULT_LINE_IDS), "unsupported lines")
    bronze_root, mapping_file = Path(bronze_root), Path(mapping_file)
    expected = {}
    for offset in range(days):
        current = start + timedelta(days=offset)
        for line in lines:
            for batch in range(4):
                batch_id = f"BATCH_{current:%Y%m%d}_{batch + 1:02d}_{line}"
                expected[batch_id] = (line, datetime.combine(current, datetime.min.time(), timezone.utc)
                                      + timedelta(hours=1 + 2 * batch))

    files = {table: sorted((bronze_root / table).rglob("*.parquet"))
             for table in ("mes_lims", "sensor_telemetry")}
    for table, paths in files.items():
        require(len(paths) == days * len(lines), f"unexpected number of {table} files: {len(paths)}")

    mes_frames = []
    for path in files["mes_lims"]:
        frame = read_verified(path, BRONZE_MES_LIMS_SCHEMA)
        require(frame.height == 4 and frame["line_id"].n_unique() == 1, f"MES rows/line mismatch: {path}")
        mes_frames.append(frame)
    mes = pl.concat(mes_frames)
    require(mes["batch_id"].n_unique() == mes.height, "duplicate MES batch IDs")
    require(set(mes["batch_id"]) == set(expected), "MES batch IDs do not match the requested window")
    for row in mes.iter_rows(named=True):
        line, begin = expected[row["batch_id"]]
        require(row["line_id"] == line, f"MES line mismatch: {row['batch_id']}")
        require(datetime.fromisoformat(row["production_start"]) == begin
                and datetime.fromisoformat(row["production_end"]) == begin + timedelta(hours=2),
                f"MES production window mismatch: {row['batch_id']}")

    mapping = {}
    maps = json.loads(mapping_file.read_text(encoding="utf-8"))
    require(isinstance(maps, list), "mapping must be a list")
    for group in maps:
        require(isinstance(group, dict), "mapping group must be an object")
        for batch, info in group.items():
            require(isinstance(info, dict), f"invalid mapping for {batch}")
            require(batch not in mapping or mapping[batch] == info, f"conflicting mapping for {batch}")
            mapping[batch] = info
    require(set(mapping) == set(expected), "mapping batch IDs do not match MES")
    for batch, info in mapping.items():
        line, begin = expected[batch]
        require(info.get("farm_id") in FARMS and info.get("line_id") == line
                and info.get("production_date") == begin.date().isoformat(), f"mapping lineage mismatch: {batch}")

    summaries = []
    seen = set()
    sensor_rows = 0
    for path in files["sensor_telemetry"]:
        frame = read_verified(path, BRONZE_SENSOR_TELEMETRY_SCHEMA)
        require(frame.height == 28800 and frame["line_id"].n_unique() == 1,
                f"sensor rows/line mismatch: {path}")
        for (batch,), part in frame.partition_by("batch_id", as_dict=True).items():
            require(batch in expected and batch not in seen, f"unknown or duplicate sensor batch: {batch}")
            line, begin = expected[batch]
            require(part["line_id"].unique().to_list() == [line], f"sensor line mismatch: {batch}")
            times = part["timestamp"].str.to_datetime(time_zone="UTC").sort()
            require(part.height == 7200 and times[0] == begin
                    and times[-1] == begin + timedelta(seconds=7199)
                    and np.all(np.diff(times.dt.timestamp("us").to_numpy()) == 1_000_000),
                    f"sensor timestamp sequence mismatch: {batch}")
            summaries.append({"batch_id": batch, "uht_avg": part["uht_temp"].mean()})
            seen.add(batch)
        sensor_rows += frame.height
    require(seen == set(expected), "sensor batch IDs do not match MES")
    joined = mes.join(pl.DataFrame(summaries), on="batch_id")
    quality_columns = ["fat_in", "protein_in", "acidity_sh", "yield_recovery_pct"]
    unique_quality = mes.select(quality_columns).unique().height
    require(unique_quality == len(expected), "duplicate MES quality tuples")
    fat, uht = joined["fat_in"].to_numpy(), joined["uht_avg"].to_numpy()
    require(np.std(fat) > 0 and np.std(uht) > 0, "correlation requires nonzero variance in fat and UHT")
    correlation = float(np.corrcoef(fat, uht)[0, 1])
    require(np.isfinite(correlation), "correlation must be finite")
    return {
        "status": "PASS", "check": "synthetic_bronze_acceptance",
        "bronze_root": str(bronze_root.resolve()), "mapping_file": str(mapping_file.resolve()),
        "start_date": start.isoformat(), "days": days, "lines": list(lines), "declared_seed": seed,
        "mes_files": len(files["mes_lims"]), "sensor_files": len(files["sensor_telemetry"]),
        "checksums_verified": sum(len(paths) for paths in files.values()),
        "mes_rows": mes.height, "sensor_rows": sensor_rows, "batches": len(expected),
        "unique_quality_rows": unique_quality, "defect_count": mes["batch_defect_flag"].sum(),
        "defect_rate": mes["batch_defect_flag"].mean(),
        "fat_uht_correlation": correlation,
        "silver_scope": "Not inspected by this Bronze verifier",
    }


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--bronze-root", type=Path, required=True)
    parser.add_argument("--mapping-file", type=Path, required=True)
    parser.add_argument("--start-date", type=date.fromisoformat, default=date(2025, 1, 1))
    parser.add_argument("--days", type=int, default=90)
    parser.add_argument("--line-id", default=",".join(DEFAULT_LINE_IDS))
    parser.add_argument("--seed", type=int, default=5425, help="Declared generator seed for provenance")
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    if (args.output.resolve().is_relative_to(args.bronze_root.resolve())
            or args.output.resolve() == args.mapping_file.resolve()):
        parser.error("--output must be outside Bronze and must not overwrite the mapping")
    try:
        result = verify_dataset(args.bronze_root, args.mapping_file, start=args.start_date, days=args.days,
                                lines=[line.strip() for line in args.line_id.split(",")], seed=args.seed)
    except (OSError, ValueError, pl.exceptions.PolarsError) as exc:
        result = {"status": "FAIL", "check": "synthetic_bronze_acceptance", "error": str(exc)}
    try:
        revision = subprocess.check_output(["git", "rev-parse", "HEAD"], text=True,
                                           cwd=Path(__file__).resolve().parents[1]).strip()
    except (OSError, subprocess.CalledProcessError):
        revision = "unknown"
    result.update({"revision": revision, "platform": platform.platform(),
                   "python": platform.python_version(), "numpy": np.__version__, "polars": pl.__version__,
                   "verified_at": datetime.now(timezone.utc).isoformat()})
    repo = Path(__file__).resolve().parents[1]
    result["generator_sha256"] = sha256_file(repo / "src/lakehouse_storage/generate_mes_data.py")
    result["verifier_sha256"] = sha256_file(Path(__file__))
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(result, indent=2, allow_nan=False), encoding="utf-8")
    print(json.dumps(result, allow_nan=False))
    return 0 if result["status"] == "PASS" else 1


if __name__ == "__main__":
    raise SystemExit(main())
