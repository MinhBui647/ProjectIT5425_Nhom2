import argparse
import json
from datetime import date
from pathlib import Path

import polars as pl
from deltalake import DeltaTable

from lakehouse_storage import FARM_LOCATIONS, get_silver_table_uri
from lakehouse_storage.config import GOLD_LOCAL_PATH, get_storage_options
from olap_engineering.star_schema import GOLD_TABLES, column_names

LOCAL_TZ = "Asia/Ho_Chi_Minh"
BATCH_FARM_MAP = Path("data/synthetic/batch_farm_map.json")

# Silver is written by TV3 (data_governance.run_governance): only rows that
# passed the quality gate are committed, each tagged with its gate status.
GOVERNED_STATUSES = ["PASS", "PASS_WITH_REJECTIONS"]
GOVERNANCE_COLUMNS = ["_quality_status", "_contract_version"]
# Columns TV3 derives during Bronze -> Silver; TV4 reads them, never recomputes.
TV3_DERIVED_COLUMNS = {
    "sensor_telemetry": ["is_anomaly_flag"],      # Hampel OR rolling Z-score
    "weather": ["thi_index", "is_heat_stress"],   # THI, stress when THI > 72
}
# TV3 keeps FAO "DAIRY" from long Bronze and "Dairy Price Index" from wide Bronze.
FAO_DAIRY_INDEX_NAMES = ["DAIRY", "Dairy Price Index"]


# ---------------------------------------------------------------- helpers

def silver_uri(table: str, silver_root: str | None = None) -> str:
    """Same layout as run_governance --silver-root; default is MinIO."""
    if silver_root is None:
        return get_silver_table_uri(table)
    root = silver_root.rstrip("/\\")
    return f"{root}/{table}" if root.startswith("s3://") else str((Path(root) / table).resolve())


def require_governed(lf: pl.LazyFrame, table: str) -> pl.LazyFrame:
    """Accept only Silver produced by TV3 and keep gate-approved rows."""
    names = lf.collect_schema().names()
    missing = [c for c in GOVERNANCE_COLUMNS + TV3_DERIVED_COLUMNS.get(table, []) if c not in names]
    if missing:
        raise SystemExit(
            f"Silver {table} is missing {missing}: load it with "
            f"'python -m data_governance.run_governance --table {table}' (TV3) first."
        )
    return lf.filter(pl.col("_quality_status").is_in(GOVERNED_STATUSES))


def scan_silver(table: str, silver_root: str | None = None) -> pl.LazyFrame | None:
    uri = silver_uri(table, silver_root)
    opts = get_storage_options()
    if not DeltaTable.is_deltatable(uri, storage_options=opts):
        print(f"[skip] Silver table not found: {uri}")
        return None
    return require_governed(pl.scan_delta(uri, storage_options=opts), table)


def _local(col: str) -> pl.Expr:
    return pl.col(col).dt.convert_time_zone(LOCAL_TZ)


def date_key_expr(col: str) -> pl.Expr:
    return _local(col).dt.strftime("%Y%m%d").cast(pl.UInt32)


def time_key_expr(col: str) -> pl.Expr:
    return _local(col).dt.hour().cast(pl.UInt16) * 100 + _local(col).dt.minute().cast(pl.UInt16)


def _with_key(df: pl.DataFrame, key: str, dtype) -> pl.DataFrame:
    return df.with_row_index(key, offset=1).with_columns(pl.col(key).cast(dtype))


def _finalize(df: pl.DataFrame, table: str) -> pl.DataFrame:
    """Add missing columns as null, keep schema order, shrink floats to Float32."""
    for name in column_names(table):
        if name not in df.columns:
            df = df.with_columns(pl.lit(None, dtype=pl.Float32).alias(name))
    return df.select(column_names(table)).with_columns(pl.col(pl.Float64).cast(pl.Float32))


def load_batch_farm_map(path: Path = BATCH_FARM_MAP) -> dict[str, str]:
    if not path.exists():
        return {}
    days = json.loads(path.read_text(encoding="utf-8"))  # list: one dict per day
    return {batch: info["farm_id"] for day in days for batch, info in day.items()}


# ------------------------------------------------------------- dimensions

def build_dim_date(start: date, end: date) -> pl.DataFrame:
    d = pl.col("full_date")
    return (
        pl.DataFrame({"full_date": pl.date_range(start, end, "1d", eager=True)})
        .with_columns(
            date_key=d.dt.strftime("%Y%m%d").cast(pl.UInt32),
            day=d.dt.day().cast(pl.UInt8),
            month=d.dt.month().cast(pl.UInt8),
            quarter=d.dt.quarter().cast(pl.UInt8),
            year=d.dt.year().cast(pl.UInt16),
            day_of_week=d.dt.weekday().cast(pl.UInt8),  # 1 = Monday ... 7 = Sunday
            is_weekend=(d.dt.weekday() >= 6).cast(pl.UInt8),
        )
        .select(column_names("dim_date"))
    )


def build_dim_time() -> pl.DataFrame:
    h = pl.col("hour")
    return (
        pl.DataFrame({"hour": [x for x in range(24) for _ in range(60)], "minute": list(range(60)) * 24})
        .with_columns(
            time_key=(h * 100 + pl.col("minute")).cast(pl.UInt16),
            shift_code=pl.when(h.is_between(6, 13)).then(pl.lit("MORNING"))
            .when(h.is_between(14, 21)).then(pl.lit("AFTERNOON"))
            .otherwise(pl.lit("NIGHT")),
        )
        .with_columns(h.cast(pl.UInt8), pl.col("minute").cast(pl.UInt8))
        .select(column_names("dim_time"))
    )


def build_dim_line(sensor: pl.LazyFrame, mes: pl.DataFrame) -> pl.DataFrame:
    ids = pl.concat([sensor.select("line_id"), mes.lazy().select("line_id")]).unique().sort("line_id")
    return _with_key(ids.collect(), "line_key", pl.UInt16).select(column_names("dim_line"))


def build_dim_product(mes: pl.DataFrame) -> pl.DataFrame:
    ids = mes.select("sku_id").unique().sort("sku_id")
    return _with_key(ids, "product_key", pl.UInt16).select(column_names("dim_product"))


def build_dim_batch(mes: pl.DataFrame, sensor: pl.LazyFrame) -> pl.DataFrame:
    from_mes = mes.select("batch_id", "production_start", "production_end", "microbiology_status")
    from_sensor = sensor.select("batch_id").drop_nulls().unique().collect()
    df = (
        pl.concat([from_mes, from_sensor], how="diagonal")
        .unique("batch_id", keep="first", maintain_order=True)  # MES attributes win
        .sort("batch_id")
    )
    return _with_key(df, "batch_key", pl.UInt32).select(column_names("dim_batch"))


def build_dim_farm(valid_from: date) -> pl.DataFrame:
    rows = [
        {"farm_id": k, "farm_name": v["name"], "latitude": v["lat"], "longitude": v["lon"]}
        for k, v in sorted(FARM_LOCATIONS.items())
    ]
    df = pl.DataFrame(rows).with_columns(
        valid_from=pl.lit(valid_from),
        valid_to=pl.lit(None, dtype=pl.Date),  # SCD2: null = current version
        is_current=pl.lit(1, dtype=pl.UInt8),
    )
    return _finalize(_with_key(df, "farm_key", pl.UInt16), "dim_farm")


# ------------------------------------------------------------------ facts

def build_fact_sensor(sensor: pl.LazyFrame, dim_line: pl.DataFrame, dim_batch: pl.DataFrame) -> pl.LazyFrame:
    return (
        # TV3 rejects duplicates inside one Bronze file; re-landed files can still overlap.
        sensor.unique(["line_id", "timestamp"], keep="last")
        .join(dim_line.lazy().select("line_id", "line_key"), on="line_id", how="left")
        .join(dim_batch.lazy().select("batch_id", "batch_key"), on="batch_id", how="left")
        .with_columns(
            date_key=date_key_expr("timestamp"),
            time_key=time_key_expr("timestamp"),
            is_anomaly_flag=pl.col("is_anomaly_flag").fill_null(False).cast(pl.UInt8),  # TV3 bool -> 0/1
        )
        .select(column_names("fact_sensor_telemetry"))
    )


def build_fact_batch(mes: pl.DataFrame, fact_sensor: pl.LazyFrame, dims: dict, batch_farm: dict) -> pl.DataFrame:
    stats = (
        fact_sensor.filter(pl.col("batch_key").is_not_null())
        .group_by("batch_key")
        .agg(
            n_readings=pl.len().cast(pl.UInt32),
            uht_temp_avg=pl.col("uht_temp").mean(),
            uht_temp_std=pl.col("uht_temp").std(),
            uht_temp_min=pl.col("uht_temp").min(),
            uht_temp_max=pl.col("uht_temp").max(),
            flow_rate_avg=pl.col("flow_rate").mean(),
            power_kwh=pl.col("power_kw").sum() / 3600,  # 1 sample/second
            n_anomalies=pl.col("is_anomaly_flag").cast(pl.UInt32).sum(),
        )
    )
    if "farm_id" not in mes.columns:
        mes = mes.with_columns(
            farm_id=pl.col("batch_id").replace_strict(batch_farm, default=None, return_dtype=pl.String)
        )
    df = (
        mes.lazy()
        .join(dims["dim_batch"].lazy().select("batch_id", "batch_key"), on="batch_id")
        .join(dims["dim_line"].lazy().select("line_id", "line_key"), on="line_id", how="left")
        .join(dims["dim_product"].lazy().select("sku_id", "product_key"), on="sku_id", how="left")
        .join(dims["dim_farm"].lazy().select("farm_id", "farm_key"), on="farm_id", how="left")
        .join(stats, on="batch_key", how="left")
        .with_columns(
            date_key=date_key_expr("production_start"),
            start_time_key=time_key_expr("production_start"),
            duration_min=(pl.col("production_end") - pl.col("production_start")).dt.total_seconds() / 60,
            batch_defect_flag=pl.col("batch_defect_flag").cast(pl.UInt8),
            n_readings=pl.col("n_readings").fill_null(0),
            n_anomalies=pl.col("n_anomalies").fill_null(0),
        )
        .collect()
    )
    return _finalize(df, "fact_batch_production")


def build_fact_external(weather, prices, indices, dim_farm: pl.DataFrame) -> pl.DataFrame | None:
    if weather is None:
        print("[skip] fact_external_market_weather: no Silver weather (run data_governance.run_governance --table weather)")
        return None
    daily = (
        weather.with_columns(full_date=_local("observed_at").dt.date())
        .group_by("farm_id", "full_date")
        .agg(
            temperature_avg_c=pl.col("temperature_c").mean(),
            humidity_avg_pct=pl.col("relative_humidity_pct").mean(),
            precipitation_mm=pl.col("precipitation_mm").sum(),
            thi_index=pl.col("thi_index").max(),  # daily peak of TV3's hourly THI
            is_heat_stress=pl.col("is_heat_stress").any(),  # any stressed hour (TV3 rule)
        )
        .with_columns(pl.col("is_heat_stress").cast(pl.UInt8))
        .sort("full_date")
        .collect()
    )
    if prices is not None:
        gdt = (
            prices.filter(pl.col("product").is_in(["WMP", "SMP", "AMF"]))
            .with_columns(full_date=_local("observed_at").dt.date())
            .group_by("full_date", "product")
            .agg(pl.col("price").mean())
            .collect()
            .pivot(on="product", index="full_date", values="price")
            .rename(lambda c: c if c == "full_date" else f"gdt_{c.lower()}_usd")
            .sort("full_date")
        )
        # GDT auctions are every ~2 weeks: carry the last known price forward.
        daily = daily.join_asof(gdt, on="full_date", strategy="backward")
    if indices is not None:
        fao = (
            indices.filter(pl.col("index_name").is_in(FAO_DAIRY_INDEX_NAMES))
            .select(full_date=pl.col("period_start"), fao_dairy_index=pl.col("index_value"))
            .sort("full_date")
            .unique("full_date", keep="last", maintain_order=True)  # same month in several crawls
            .collect()
        )
        daily = daily.join_asof(fao, on="full_date", strategy="backward")
    df = daily.join(dim_farm.select("farm_id", "farm_key"), on="farm_id", how="inner").with_columns(
        date_key=pl.col("full_date").dt.strftime("%Y%m%d").cast(pl.UInt32)
    )
    return _finalize(df, "fact_external_market_weather")


# ------------------------------------------------------------------- main

def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--out", default=GOLD_LOCAL_PATH)
    parser.add_argument("--date-from", default="2024-01-01")
    parser.add_argument("--date-to", default="2027-12-31")
    parser.add_argument("--silver-root", default=None,
                        help="Same value as run_governance --silver-root (default: MinIO)")
    parser.add_argument("--upload-minio", action="store_true",
                        help="Also upload the Gold Parquet files to s3://<bucket>/03_gold_marts/")
    args = parser.parse_args()
    out = Path(args.out)
    out.mkdir(parents=True, exist_ok=True)

    def write(df: pl.DataFrame, name: str) -> None:
        df.write_parquet(out / f"{name}.parquet")
        print(f"{name:32s} {df.height:>10,} rows")

    def scan(table: str) -> pl.LazyFrame | None:
        return scan_silver(table, args.silver_root)

    sensor = scan("sensor_telemetry")
    mes_lf = scan("mes_lims")
    if sensor is None or mes_lf is None:
        raise SystemExit("Need governed Silver sensor_telemetry and mes_lims first (HuongdanChayProject.md, step 5).")
    # SCD Type 1: a batch re-landed in a newer Bronze file overrides the older one.
    mes = mes_lf.sort("_ingested_at").unique("batch_id", keep="last", maintain_order=True).collect()

    dims = {
        "dim_date": build_dim_date(date.fromisoformat(args.date_from), date.fromisoformat(args.date_to)),
        "dim_time": build_dim_time(),
        "dim_line": build_dim_line(sensor, mes),
        "dim_product": build_dim_product(mes),
        "dim_batch": build_dim_batch(mes, sensor),
        "dim_farm": build_dim_farm(date.fromisoformat(args.date_from)),
    }
    for name, df in dims.items():
        write(_finalize(df, name), name)

    fact_sensor = _finalize(build_fact_sensor(sensor, dims["dim_line"], dims["dim_batch"]).collect(), "fact_sensor_telemetry")
    write(fact_sensor, "fact_sensor_telemetry")
    write(build_fact_batch(mes, fact_sensor.lazy(), dims, load_batch_farm_map()), "fact_batch_production")

    ext = build_fact_external(scan("weather"), scan("market_prices"), scan("market_indices"), dims["dim_farm"])
    if ext is not None:
        write(ext, "fact_external_market_weather")

    if args.upload_minio:
        from olap_engineering.gold_store import upload_gold_dir

        for key, size in upload_gold_dir(out).items():
            print(f"[minio] {key:48s} {size:>12,} bytes")


if __name__ == "__main__":
    main()