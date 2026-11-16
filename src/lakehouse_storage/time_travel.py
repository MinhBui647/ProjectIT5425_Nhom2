"""Time-travel queries for ISO 22000 traceability."""

import logging
from datetime import date, datetime, timezone
from pathlib import Path
from typing import TYPE_CHECKING

import polars as pl

from lakehouse_storage.config import TIMEZONE, get_storage_options
from lakehouse_storage.utils import sha256_file

if TYPE_CHECKING:
    from deltalake import DeltaTable

logger = logging.getLogger(__name__)


def load_version(table_uri: str, version: int) -> pl.DataFrame:
    """Load table as of specific Delta version."""
    lf = pl.scan_delta(table_uri, version=version, storage_options=get_storage_options())
    return lf.collect()


def get_table_history(table_uri: str, limit: int = 10) -> pl.DataFrame:
    """Get table commit history."""
    dt = _get_delta_table(table_uri)
    history = dt.history(limit=limit)
    if not history:
        return pl.DataFrame()
    return pl.DataFrame(history)


def load_as_of(table_uri: str, timestamp_iso: str | int) -> pl.DataFrame:
    """Load table as of specific timestamp."""
    if isinstance(timestamp_iso, (int, float)):
        # Epoch milliseconds from Delta history
        moment = datetime.fromtimestamp(timestamp_iso / 1000, tz=timezone.utc)
    else:
        try:
            moment = datetime.fromisoformat(str(timestamp_iso))
            if moment.tzinfo is None:
                moment = moment.replace(tzinfo=timezone.utc)
        except ValueError as e:
            raise ValueError(f"Invalid timestamp {timestamp_iso!r}") from e

    lf = pl.scan_delta(table_uri, version=moment, storage_options=get_storage_options())
    return lf.collect()


def diff_versions(
    table_uri: str,
    v1: int,
    v2: int,
    id_column: str | None = None,
) -> dict:
    """Compare two versions and return differences.

    Args:
        table_uri: S3 URI of the Delta table
        v1: First version number
        v2: Second version number
        id_column: Primary key column (auto-detected if not provided)
    """
    df_v1 = load_version(table_uri, v1)
    df_v2 = load_version(table_uri, v2)

    # Auto-detect ID column if not provided. Prefer temporal keys and skip
    # columns that are entirely NULL (e.g. batch_id before MES backfill).
    if id_column is None:
        candidates = [
            "timestamp", "observed_at", "period_start",
            "batch_id", "recall_id", "file",
        ]
        for col in candidates:
            if col in df_v1.columns and df_v1[col].null_count() < df_v1.height:
                id_column = col
                break
        else:
            id_column = next(
                (c for c in df_v1.columns if df_v1[c].null_count() < df_v1.height),
                df_v1.columns[0],
            )

    if id_column not in df_v1.columns:
        raise ValueError(f"ID column '{id_column}' not found in table")

    v1_ids = set(df_v1.select(id_column).to_series().to_list())
    v2_ids = set(df_v2.select(id_column).to_series().to_list())

    added = df_v2.filter(~pl.col(id_column).is_in(v1_ids))
    removed = df_v1.filter(~pl.col(id_column).is_in(v2_ids))

    return {
        "added": added,
        "removed": removed,
        "counts": {
            "v1_rows": len(df_v1),
            "v2_rows": len(df_v2),
            "added_count": len(added),
            "removed_count": len(removed),
        },
        "id_column": id_column,
    }


def trace_batch(table_uri: str, batch_id: str, id_column: str = "batch_id") -> dict:
    """Trace a batch to its source Bronze file for ISO 22000 audit."""
    lf = pl.scan_delta(table_uri, storage_options=get_storage_options())

    col_dtype = lf.collect_schema().get(id_column)
    is_temporal = isinstance(col_dtype, pl.Datetime) or col_dtype == pl.Date
    if isinstance(batch_id, str) and is_temporal:
        key = _parse_temporal_id(batch_id, col_dtype)
        if key is None:
            raise ValueError(f"Batch not found: {batch_id}")
        batch = lf.filter(pl.col(id_column) == key).collect()
    else:
        batch = lf.filter(pl.col(id_column) == batch_id).collect()

    if len(batch) == 0:
        raise ValueError(f"Batch not found: {batch_id}")

    if "_bronze_file" not in batch.columns:
        raise ValueError("Lineage column _bronze_file not found")

    sources = []
    hashes_by_path = {}
    for path, expected in batch.select("_bronze_file", "_bronze_sha256").iter_rows():
        hashes_by_path.setdefault(path, set()).add(expected)

    actual_by_path = {}
    for row in batch.select("_bronze_file", "_bronze_sha256", "_ingested_at").unique(
        subset=["_bronze_file", "_bronze_sha256"], maintain_order=True
    ).iter_rows(named=True):
        path, expected = row["_bronze_file"], row["_bronze_sha256"]
        actual, reason = None, None
        if path not in actual_by_path:
            try:
                if not path:
                    reason = "missing_path"
                elif not Path(path).is_file():
                    reason = "missing_file"
                else:
                    actual = sha256_file(Path(path))
            except OSError as exc:
                reason = f"unreadable_file: {exc}"
            actual_by_path[path] = (actual, reason)
        actual, reason = actual_by_path[path]
        if len(hashes_by_path[path]) > 1:
            reason = "conflicting_expected_hashes"
        elif not expected:
            reason = reason or "missing_expected_hash"
        elif actual != expected:
            reason = reason or "checksum_mismatch"
        sources.append({
            "bronze_file": path,
            "bronze_sha256": expected,
            "actual_sha256": actual,
            "ingested_at": str(row["_ingested_at"]) if row["_ingested_at"] else None,
            "integrity_verified": reason is None,
            "reason": reason,
        })

    representative = sources[0]
    return {
        "batch_id": batch_id,
        "batch_info": batch.to_dict(as_series=False),
        "lineage": {key: representative[key] for key in (
            "bronze_file", "bronze_sha256", "actual_sha256", "ingested_at"
        )},
        "lineage_sources": sources,
        "integrity_verified": all(source["integrity_verified"] for source in sources),
    }


def trace_event(table_uri: str, event_id: str) -> dict:
    """Trace a sensor event to its source."""
    return trace_batch(table_uri, event_id, id_column="timestamp")


def trace_batch_history(table_uri: str, batch_id: str, id_column: str = "batch_id") -> pl.DataFrame:
    """Get all historical versions where a batch appeared."""
    dt = _get_delta_table(table_uri)
    history = dt.history()

    results = []
    for entry in history:
        version = entry["version"]
        try:
            batch = (
                pl.scan_delta(table_uri, version=version, storage_options=get_storage_options())
                .filter(pl.col(id_column) == batch_id)
                .collect()
            )
            if len(batch) > 0:
                row = batch.to_dicts()[0]
                row["_version"] = version
                row["_commit_timestamp"] = entry["timestamp"]
                results.append(row)
        except Exception as e:
            logger.debug("Could not inspect version %d: %s", version, e)

    return pl.DataFrame(results) if results else pl.DataFrame()


def generate_audit_report(
    table_uri: str,
    date_from: str,
    date_to: str,
    output_path: str | Path | None = None,
) -> pl.DataFrame:
    """Generate ISO 22000 audit report for date range."""
    date_from = date.fromisoformat(date_from)
    date_to = date.fromisoformat(date_to)

    df = pl.scan_delta(table_uri, storage_options=get_storage_options())
    df = df.filter(
        pl.col("ingestion_date") >= pl.lit(date_from),
        pl.col("ingestion_date") <= pl.lit(date_to),
    )

    # Standard audit columns
    audit_cols = ["_bronze_file", "_bronze_sha256", "_ingested_at", "ingestion_date"]
    schema_names = df.collect_schema().names()

    # Add identifier column if available
    for col in ["batch_id", "recall_id", "timestamp", "observed_at"]:
        if col in schema_names:
            audit_cols.insert(0, col)
            break

    available = [c for c in audit_cols if c in schema_names]
    df = df.select(available)
    result = df.collect()

    if output_path:
        output_path = Path(output_path)
        output_path.parent.mkdir(parents=True, exist_ok=True)
        result.write_parquet(output_path)
        logger.info("Audit report saved to %s", output_path)

    return result


def get_latest_version(table_uri: str) -> int:
    """Get current (latest) version number."""
    return _get_delta_table(table_uri).version()


def get_version_at_date(table_uri: str, target_date: str) -> int | None:
    """Get the latest eligible commit; naive target timestamps mean UTC."""
    target = _parse_timestamp(target_date)
    dt = _get_delta_table(table_uri)
    history = dt.history()

    eligible = [
        (_parse_timestamp(entry["timestamp"]), entry["version"])
        for entry in history
        if _parse_timestamp(entry["timestamp"]) <= target
    ]
    return max(eligible)[1] if eligible else None


def _get_delta_table(table_uri: str) -> "DeltaTable":
    """Get DeltaTable object."""
    from deltalake import DeltaTable
    return DeltaTable(table_uri, storage_options=get_storage_options())


def _parse_temporal_id(value: str, dtype):
    """Parse a string ID into a value matching a Date/Datetime column.

    Naive strings are interpreted in the factory timezone, matching how Bronze
    timestamps are localized during ingest. Returns None if unparseable.
    """
    from zoneinfo import ZoneInfo

    try:
        parsed = datetime.fromisoformat(value)
    except ValueError:
        return None

    if dtype == pl.Date:
        return parsed.date()

    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=ZoneInfo(TIMEZONE))

    tz_name = dtype.time_zone
    if tz_name is not None:
        parsed = parsed.astimezone(ZoneInfo(tz_name))
    return parsed


def _parse_timestamp(value) -> datetime:
    """Normalize a Delta history timestamp."""
    if isinstance(value, datetime):
        return value if value.tzinfo else value.replace(tzinfo=timezone.utc)
    if isinstance(value, (int, float)):
        return datetime.fromtimestamp(value / 1000.0, tz=timezone.utc)
    parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    return parsed if parsed.tzinfo else parsed.replace(tzinfo=timezone.utc)
