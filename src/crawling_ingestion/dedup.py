"""Record-level deduplication for local crawler Bronze batches.

Bronze files are the durable source of truth. Rebuild the latest fingerprint
per business key from them, so retries do not depend on a separate index commit.
Only this writer's callers participate in the per-table OS lock.
"""
from contextlib import contextmanager
from datetime import date, datetime, timezone
from hashlib import sha256
import json
import math
import os
from pathlib import Path

import polars as pl

from lakehouse_storage.bronze_writer import verify_checksum, write_bronze_batch
from lakehouse_storage.schemas import get_bronze_schema, get_source_name

KEYS = {
    "weather": ("farm_id", "observed_at"),
    "market_prices": ("ProductGroupCode", "EventNumber"),
    "food_recalls": ("recall_number",),
    "usda_market_prices": ("source", "product", "contract", "observed_at", "currency", "unit"),
    "market_indices": ("source", "index_name", "period_start"),
}
DATE_COLUMN = {
    "weather": "observed_at", "market_prices": "EventDate",
    "food_recalls": "report_date", "usda_market_prices": "observed_at",
    "market_indices": "period_start",
}
# These describe a fetch/page, not a source record. Keep them in the Parquet,
# but changing them alone must not produce a new Bronze revision.
FETCH_COLUMNS = {"ingested_at", "meta_last_updated", "meta_results_skip",
                 "meta_results_limit", "meta_results_total"}
JSON_COLUMNS = {"_raw_record", "openfda"}
FINGERPRINT_VERSION = "crawler-record-v1"


def _canonical(value):
    if isinstance(value, dict):
        return {k: _canonical(v) for k, v in value.items()}
    if isinstance(value, (list, tuple)):
        return [_canonical(v) for v in value]
    if isinstance(value, float):
        if not math.isfinite(value):
            return {"__nonfinite_float__": str(value)}
        # Parquet can promote integers to floats across batches.
        return int(value) if value.is_integer() else value
    if isinstance(value, (datetime, date)):
        return value.isoformat()
    return value


def _hash(value):
    text = json.dumps(_canonical(value), sort_keys=True, separators=(",", ":"),
                      ensure_ascii=False, allow_nan=False)
    return sha256(text.encode("utf-8")).hexdigest()


def _date_value(value, field):
    if value is None:
        raise ValueError(f"Missing date field: {field}")
    text = value.isoformat() if isinstance(value, (date, datetime)) else str(value)
    if field in {"report_date", "period_start"}:
        if len(text) == 8 and text.isdigit():
            return datetime.strptime(text, "%Y%m%d").date().isoformat()
        return date.fromisoformat(text).isoformat()
    stamp = datetime.fromisoformat(text.replace("Z", "+00:00"))
    # The default weather crawler returns naive UTC timestamps.
    if stamp.tzinfo is None:
        stamp = stamp.replace(tzinfo=timezone.utc)
    return stamp.astimezone(timezone.utc).isoformat()


def record_fingerprints(row: dict, table_name: str) -> tuple[str, str]:
    """Return (business-key hash, source-content hash), independent of fetch time."""
    keys = KEYS[table_name]
    if any(row.get(k) is None or row.get(k) == "" for k in keys):
        raise ValueError(f"Missing business key for {table_name}: {keys}")
    payload = {}
    for name, value in row.items():
        if name in FETCH_COLUMNS or (name.startswith("_") and name != "_raw_record"):
            continue
        if name in JSON_COLUMNS and isinstance(value, str):
            try:
                value = json.loads(value)
            except json.JSONDecodeError as exc:
                raise ValueError(f"Invalid JSON in {name}") from exc
        if name == DATE_COLUMN[table_name]:
            value = _date_value(value, name)
        payload[name] = value
    key = _hash([table_name, {name: payload[name] for name in keys}])
    fingerprint = _hash([FINGERPRINT_VERSION, table_name, payload])
    return key, fingerprint


@contextmanager
def _table_lock(root: Path, table_name: str):
    folder = root / ".crawler_locks"
    folder.mkdir(parents=True, exist_ok=True)
    with (folder / (table_name + ".lock")).open("a+b") as stream:
        stream.seek(0, 2)
        if not stream.tell():
            stream.write(b"0")
            stream.flush()
        stream.seek(0)
        try:
            if os.name == "nt":
                import msvcrt
                msvcrt.locking(stream.fileno(), msvcrt.LK_NBLCK, 1)
            else:
                import fcntl
                fcntl.flock(stream, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except OSError as exc:
            raise RuntimeError(f"Another crawler is writing {table_name}; retry later") from exc
        try:
            yield
        finally:
            stream.seek(0)
            if os.name == "nt":
                msvcrt.locking(stream.fileno(), msvcrt.LK_UNLCK, 1)
            else:
                fcntl.flock(stream, fcntl.LOCK_UN)


def _load_latest(root: Path, table_name: str) -> dict[str, str]:
    latest = {}
    # Existing writer names files with their UTC write timestamp. Sort by name,
    # not the event-day directory: corrections may move a record to another day.
    files = sorted((root / get_source_name(table_name)).rglob("*.parquet"),
                   key=lambda p: (p.name, str(p)))
    for file in files:
        if not verify_checksum(file):
            raise ValueError(f"Bronze checksum missing/mismatched: {file}; no new data written")
        for row in pl.read_parquet(file).iter_rows(named=True):
            key, fingerprint = record_fingerprints(row, table_name)
            latest[key] = fingerprint
    return latest


def write_unique_bronze(df: pl.DataFrame, table_name: str, bronze_root) -> dict:
    """Append new/changed records, skip unchanged records and exact batch duplicates.

    Accepts the five native Python crawler schemas. Preserves old files and
    revisions, including a change A -> B -> A. Conflicting values for one key
    in a single call are rejected rather than arbitrarily choosing a winner.
    No crawling, distributed lock, Silver merge or historical cleanup here.
    """
    if table_name not in KEYS:
        raise ValueError(f"Unsupported crawler table: {table_name}")
    if str(bronze_root).startswith("s3://"):
        raise ValueError("Deduplication currently supports local Bronze only")
    result = {"input_rows": len(df), "written_rows": 0, "unchanged_rows": 0,
              "batch_duplicates": 0, "new_rows": 0, "revised_rows": 0, "paths": []}
    if df.is_empty():
        return result
    required = {field.name for field in get_bronze_schema(table_name) if not field.nullable}
    required.update(KEYS[table_name])
    required.add(DATE_COLUMN[table_name])
    missing = required - set(df.columns)
    if missing:
        raise ValueError(f"Missing columns for {table_name}: {sorted(missing)}")
    pending, unique = [], {}
    for index, row in enumerate(df.iter_rows(named=True)):
        key, fingerprint = record_fingerprints(row, table_name)
        day = _date_value(row[DATE_COLUMN[table_name]], DATE_COLUMN[table_name])[:10]
        if key in unique:
            if unique[key] != fingerprint:
                raise ValueError(f"Conflicting records for one {table_name} business key: {key}")
            result["batch_duplicates"] += 1
            continue
        unique[key] = fingerprint
        pending.append((index, key, fingerprint, day))
    root = Path(bronze_root).resolve()
    with _table_lock(root, table_name):
        latest = _load_latest(root, table_name)
        daily = {}
        for index, key, fingerprint, day in pending:
            if latest.get(key) == fingerprint:
                result["unchanged_rows"] += 1
                continue
            result["revised_rows" if key in latest else "new_rows"] += 1
            daily.setdefault(day, []).append((index, key, fingerprint))
        for day in sorted(daily):
            rows = daily[day]
            batch = df[[item[0] for item in rows]].with_columns(
                pl.Series("_record_key_sha256", [item[1] for item in rows]),
                pl.Series("_record_sha256", [item[2] for item in rows]),
                pl.lit(FINGERPRINT_VERSION).alias("_dedup_version"),
            )
            # Keep the source columns intact; the existing writer owns the
            # Parquet/checksum format and date partitioning.
            file = write_bronze_batch(batch, table_name, bronze_root=root)
            result["paths"].append(file)
            result["written_rows"] += len(batch)
    return result
