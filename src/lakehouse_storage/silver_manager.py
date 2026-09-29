"""Silver layer manager - Delta Lake ACID writes on MinIO."""

import logging
from datetime import date, datetime, timezone
from pathlib import Path

import polars as pl
import pyarrow as pa
from deltalake import DeltaTable, write_deltalake

from lakehouse_storage.bronze_writer import ensure_within_bronze, verify_checksum
from lakehouse_storage.config import TIMEZONE, get_storage_options
from lakehouse_storage.schemas import (
    get_partition_columns,
    get_silver_schema,
    get_silver_table_uri,
    normalize_table_name,
)
from lakehouse_storage.utils import sha256_file

logger = logging.getLogger(__name__)

# Timestamp columns for each table
TIMESTAMP_COLUMNS = {
    "sensor_telemetry": ["timestamp"],
    "mes_lims": ["production_start", "production_end"],
    "market_prices": ["observed_at"],
    "market_indices": [],  # Uses date32, not timestamp
    "weather": ["observed_at"],
    "food_recalls": ["published_at"],
}

# Date columns for ingestion_date extraction
DATE_COLUMNS = {
    "sensor_telemetry": ["timestamp"],
    "mes_lims": ["production_start"],
    "market_prices": ["observed_at"],
    "market_indices": ["period_start"],
    "weather": ["observed_at"],
    "food_recalls": ["published_at"],
}

# Float columns that need casting to float32
FLOAT_COLUMNS = {
    "sensor_telemetry": [
        "preheat_temp", "uht_temp", "homo_press_stage1", "homo_press_stage2",
        "flow_rate", "conductivity", "power_kw",
    ],
    "mes_lims": [
        "fat_in", "protein_in", "acidity_sh", "yield_recovery_pct",
    ],
    "market_prices": ["price"],
    "market_indices": ["index_value"],
    "weather": ["temperature_c", "relative_humidity_pct", "precipitation_mm"],
    "food_recalls": [],
}


def append_to_delta(
    df: pl.DataFrame,
    table_uri: str,
    partition_by: list[str],
    table_name: str | None = None,
) -> None:
    """Append DataFrame to Delta table."""
    silver_df = _to_silver_frame(df, table_name)

    if silver_df.height == 0:
        logger.warning("append_to_delta: nothing to write")
        return

    silver_schema = get_silver_schema(table_name) if table_name else None

    arrow_table = silver_df.to_arrow()
    if silver_schema and silver_df.width > 0:
        arrow_table = _cast_to_schema(arrow_table, silver_schema, strict=True)

    write_deltalake(
        table_or_uri=table_uri,
        data=arrow_table,
        mode="append",
        partition_by=partition_by,
        storage_options=get_storage_options(),
    )

    logger.info("Appended %d rows to %s", silver_df.height, table_uri)


def _verified_bronze_sha256(bronze_path: Path, enforce_checksum: bool) -> str:
    """Return the SHA-256 to record as lineage, reusing the verified sidecar.

    Verifying the sidecar already hashes the Parquet; reading the digest back
    avoids a second full read of the Bronze file.
    """
    if enforce_checksum:
        if not verify_checksum(bronze_path):
            raise ValueError(
                f"Checksum verification failed for Bronze file: {bronze_path}"
            )
        return bronze_path.with_suffix(".sha256").read_text().strip()
    return sha256_file(bronze_path)


def ingest_bronze_file(
    bronze_path: str | Path,
    silver_table_uri: str,
    partition_by: list[str],
    table_name: str | None = None,
    bronze_root: Path | str | None = None,
    enforce_checksum: bool = True,
) -> dict:
    """Read Bronze file, verify integrity, add lineage, and write to Silver."""
    bronze_path = Path(bronze_path)
    if bronze_root is not None:
        bronze_path = ensure_within_bronze(bronze_path, bronze_root)

    if not bronze_path.exists():
        raise FileNotFoundError(f"Bronze file not found: {bronze_path}")

    sha256 = _verified_bronze_sha256(bronze_path, enforce_checksum)
    df = pl.read_parquet(bronze_path)

    # Add lineage columns
    df = df.with_columns([
        pl.lit(str(bronze_path)).alias("_bronze_file"),
        pl.lit(sha256).alias("_bronze_sha256"),
    ])

    silver_df = _to_silver_frame(df, table_name)

    if silver_df.height == 0:
        return {"rows_ingested": 0, "bronze_path": str(bronze_path)}

    arrow_table = silver_df.to_arrow()
    silver_schema = get_silver_schema(table_name) if table_name else None
    if silver_schema and silver_df.width > 0:
        arrow_table = _cast_to_schema(arrow_table, silver_schema, strict=True)

    write_deltalake(
        table_or_uri=silver_table_uri,
        data=arrow_table,
        mode="append",
        partition_by=partition_by,
        storage_options=get_storage_options(),
    )

    return {
        "rows_ingested": silver_df.height,
        "bronze_path": str(bronze_path),
        "sha256": sha256,
    }


def read_silver_table(
    table_uri: str,
    line_id: str | None = None,
    date_from: str | date | None = None,
    date_to: str | date | None = None,
    columns: list[str] | None = None,
    limit: int | None = None,
) -> pl.DataFrame:
    """Read Silver table with filters."""
    lf = pl.scan_delta(table_uri, storage_options=get_storage_options())

    if line_id is not None:
        if "line_id" in lf.collect_schema().names():
            lf = lf.filter(pl.col("line_id") == line_id)
        else:
            logger.warning(
                "Ignoring line_id=%r: table %s has no 'line_id' column",
                line_id,
                table_uri,
            )

    if date_from is not None:
        date_from = date.fromisoformat(date_from) if isinstance(date_from, str) else date_from
        lf = lf.filter(pl.col("ingestion_date") >= pl.lit(date_from))

    if date_to is not None:
        date_to = date.fromisoformat(date_to) if isinstance(date_to, str) else date_to
        lf = lf.filter(pl.col("ingestion_date") <= pl.lit(date_to))

    if columns is not None:
        lf = lf.select(columns)

    if limit is not None:
        lf = lf.limit(limit)

    return lf.collect()


def export_silver_parquet(
    table_uri: str,
    dest_path: str | Path,
    line_id: str | None = None,
    date_from: str | date | None = None,
    date_to: str | date | None = None,
) -> str:
    """Export Silver table to static Parquet."""
    df = read_silver_table(table_uri, line_id=line_id, date_from=date_from, date_to=date_to)

    dest_path = Path(dest_path)
    dest_path.parent.mkdir(parents=True, exist_ok=True)
    df.write_parquet(dest_path)

    logger.info("Exported %d rows to %s", df.height, dest_path)
    return str(dest_path)


def get_delta_table(table_uri: str) -> "DeltaTable":
    """Get DeltaTable object."""
    return DeltaTable(table_uri, storage_options=get_storage_options())


def get_table_version(table_uri: str) -> int:
    """Get current version number."""
    return get_delta_table(table_uri).version()


def get_table_stats(table_uri: str) -> dict:
    """Get table statistics without materializing the full table."""
    dt = get_delta_table(table_uri)
    return {
        "version": dt.version(),
        "row_count": _count_rows(table_uri),
        "file_count": len(dt.file_uris()),
    }


def delete_from_delta(table_uri: str, predicate: str) -> dict:
    """Delete rows matching predicate."""
    dt = get_delta_table(table_uri)
    before_rows = _count_rows(table_uri)
    dt.delete(predicate=predicate)
    after_rows = _count_rows(table_uri)
    return {"rows_deleted": before_rows - after_rows, "version": dt.version()}


def _count_rows(table_uri: str) -> int:
    """Count rows via a lazy, count-only scan (no full materialization)."""
    return (
        pl.scan_delta(table_uri, storage_options=get_storage_options())
        .select(pl.len())
        .collect()
        .item()
    )


def _to_silver_frame(df: pl.DataFrame, table_name: str | None) -> pl.DataFrame:
    """Transform Bronze DataFrame to Silver contract."""
    if table_name is None:
        return df

    # Normalize/alias-resolve table name
    table_name = normalize_table_name(table_name)

    # Cast float columns to float32
    float_cols = FLOAT_COLUMNS.get(table_name, [])
    casts = [pl.col(c).cast(pl.Float32) for c in float_cols if c in df.columns]
    if casts:
        df = df.with_columns(casts)

    # Cast integer flag columns to their Silver contract type
    if table_name == "mes_lims" and "batch_defect_flag" in df.columns:
        df = df.with_columns(pl.col("batch_defect_flag").cast(pl.Int8))

    # Parse timestamp columns
    ts_cols = TIMESTAMP_COLUMNS.get(table_name, ["timestamp"])
    for ts_col in ts_cols:
        if ts_col in df.columns:
            ts_dtype = df.schema[ts_col]
            if ts_dtype == pl.String:
                df = df.with_columns(
                    pl.col(ts_col)
                    .str.to_datetime(strict=False, time_zone=TIMEZONE)
                    .alias(ts_col)
                )
            elif ts_dtype == pl.Datetime and df.schema[ts_col].time_zone is None:
                df = df.with_columns(
                    pl.col(ts_col).dt.replace_time_zone(TIMEZONE).alias(ts_col)
                )

    # Parse date columns (market_indices uses date32)
    if table_name == "market_indices" and "period_start" in df.columns:
        if df.schema["period_start"] == pl.String:
            df = df.with_columns(
                pl.col("period_start").str.to_date().alias("period_start")
            )

    # Add ingestion_date if not present
    if "ingestion_date" not in df.columns:
        date_cols = DATE_COLUMNS.get(table_name, ["timestamp"])
        for date_col in date_cols:
            if date_col in df.columns:
                col_dtype = df.schema[date_col]
                if col_dtype == pl.Datetime:
                    df = df.with_columns(pl.col(date_col).dt.date().alias("ingestion_date"))
                elif col_dtype == pl.Date:
                    df = df.with_columns(pl.col(date_col).alias("ingestion_date"))
                break
        else:
            df = df.with_columns(
                pl.lit(datetime.now(timezone.utc).date()).alias("ingestion_date")
            )

    # Add _ingested_at if not present
    if "_ingested_at" not in df.columns:
        df = df.with_columns(pl.lit(datetime.now(timezone.utc)).alias("_ingested_at"))

    return df


def _cast_to_schema(
    table: pa.Table,
    schema: pa.Schema,
    strict: bool = False,
) -> pa.Table:
    """Cast matching columns and retain extra lineage/partition columns.

    Args:
        table: Source Arrow table.
        schema: Target schema.
        strict: When True, raise if a non-nullable business column cannot be
            cast, preventing silent schema drift in the Silver layer.
    """
    target_names = set(schema.names)
    cols = {}
    for field in schema:
        if field.name in table.column_names:
            try:
                cols[field.name] = table.column(field.name).cast(field.type)
            except Exception as e:
                if strict and not field.nullable:
                    raise ValueError(
                        f"Failed to cast non-nullable column '{field.name}' "
                        f"to {field.type}: {e}"
                    ) from e
                logger.warning(
                    "Failed to cast column '%s' to %s: %s. Keeping original type.",
                    field.name,
                    field.type,
                    e,
                )
                cols[field.name] = table.column(field.name)
    for name in table.column_names:
        if name not in target_names:
            cols[name] = table.column(name)
    return pa.table(cols)


def create_delta_table(
    table_uri: str,
    schema: pa.Schema,
    partition_by: list[str] | None = None,
) -> None:
    """Create empty Delta table with schema.

    Delta requires every ``partition_by`` column to be present in the schema.
    Silver schemas only declare business columns, so partition and lineage
    columns are appended here when missing.
    """
    existing = set(schema.names)
    fields = list(schema)

    for col in partition_by or []:
        if col in existing:
            continue
        dtype = pa.date32() if col == "ingestion_date" else pa.string()
        fields.append(pa.field(col, dtype, nullable=False))
        existing.add(col)

    lineage_types = {
        "_source_system": pa.string(),
        "_bronze_file": pa.string(),
        "_bronze_sha256": pa.string(),
        "_ingested_at": pa.timestamp("us", tz=TIMEZONE),
    }
    for col, dtype in lineage_types.items():
        if col not in existing:
            fields.append(pa.field(col, dtype, nullable=True))
            existing.add(col)

    full_schema = pa.schema(fields)
    empty_table = pa.Table.from_pylist([], schema=full_schema)
    write_deltalake(
        table_or_uri=table_uri,
        data=empty_table,
        mode="error",
        partition_by=partition_by,
        storage_options=get_storage_options(),
    )
    logger.info("Created Delta table: %s", table_uri)


def write_silver(
    df: pl.DataFrame,
    table_name: str,
    source_bronze: str | Path,
    table_uri: str | None = None,
    bronze_root: Path | str | None = None,
    enforce_checksum: bool = True,
) -> int:
    """Write a preprocessed DataFrame to Silver (Delta Lake on MinIO).
    Args:
        df: Preprocessed :class:`polars.DataFrame` ready for Silver ingestion
        table_name: Canonical table name or S1-S5 alias.
        source_bronze: Path to the Bronze file from which *df* was originally read.
        table_uri: Explicit Delta table URI.  When ``None``, resolved automatically
        bronze_root: When provided, *source_bronze* must stay within this root.
        enforce_checksum: Verify the Bronze ``.sha256`` sidecar before processing.
    Returns:
        Delta table version after the write, or ``-1`` when *df* is empty.
    Raises:
        ValueError: If *table_name* is unknown, *df* fails schema enforcement
            inside ``append_to_delta``, the path escapes *bronze_root*, or the
            Bronze checksum does not match.
        FileNotFoundError: If *source_bronze* does not exist.
    """
    table_name = normalize_table_name(table_name)
    source_bronze = Path(source_bronze)

    if bronze_root is not None:
        source_bronze = ensure_within_bronze(source_bronze, bronze_root)

    if df.is_empty():
        logger.warning("write_silver: DataFrame is empty, skipping.")
        return -1

    if not source_bronze.exists():
        raise FileNotFoundError(
            f"Source Bronze file not found: {source_bronze}"
        )

    if table_uri is None:
        table_uri = get_silver_table_uri(table_name)

    partition_by = get_partition_columns(table_name)

    # Add lineage columns - same pattern as ingest_bronze_file()
    sha256 = _verified_bronze_sha256(source_bronze, enforce_checksum)
    df = df.with_columns([
        pl.lit(str(source_bronze)).alias("_bronze_file"),
        pl.lit(sha256).alias("_bronze_sha256"),
    ])

    append_to_delta(
        df=df,
        table_uri=table_uri,
        partition_by=partition_by,
        table_name=table_name,
    )

    version = get_table_version(table_uri)

    logger.info(
        "write_silver: %d rows -> %s (v%d) from %s",
        df.height, table_uri, version, source_bronze,
    )

    return version
