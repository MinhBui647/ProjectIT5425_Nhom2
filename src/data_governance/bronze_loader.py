"""Read immutable Bronze Parquet files after strict integrity checks."""

from __future__ import annotations

import re
from datetime import datetime, timezone
from pathlib import Path

import polars as pl

from data_governance.config import CONTRACT_VERSION
from data_governance.models import BronzeIdentity
from lakehouse_storage.bronze_writer import ensure_within_bronze, verify_checksum
from lakehouse_storage.schemas import normalize_table_name
from lakehouse_storage.utils import sha256_file


_BRONZE_TIMESTAMP = re.compile(r"(\d{8}_\d{6}_\d{6})(?:_\d+)?$")


def _source_timestamp(path: Path) -> datetime:
    """Read a writer timestamp from the filename, falling back to file mtime."""
    match = _BRONZE_TIMESTAMP.search(path.stem)
    if match:
        parsed = datetime.strptime(match.group(1), "%Y%m%d_%H%M%S_%f")
        return parsed.replace(tzinfo=timezone.utc)
    return datetime.fromtimestamp(path.stat().st_mtime, tz=timezone.utc)


def _table_name(path: Path, bronze_root: Path) -> str:
    relative = path.relative_to(bronze_root)
    if len(relative.parts) < 2:
        raise ValueError(
            "Bronze file must be stored below a table/source directory"
        )
    return normalize_table_name(relative.parts[0])


def load_bronze(
    bronze_path: str | Path,
    bronze_root: str | Path,
) -> tuple[pl.DataFrame, BronzeIdentity]:
    """Verify and load one local Bronze file without writing any data."""
    root = Path(bronze_root).resolve()
    path = ensure_within_bronze(bronze_path, root)

    if path.suffix != ".parquet":
        raise ValueError(f"Bronze input must use the .parquet extension: {path}")
    if not path.is_file():
        raise FileNotFoundError(f"Bronze file not found: {path}")

    sidecar = path.with_suffix(".sha256")
    if not sidecar.is_file():
        raise FileNotFoundError(f"Checksum sidecar not found: {sidecar}")
    if not verify_checksum(path):
        raise ValueError(f"Checksum mismatch for Bronze file: {path}")

    sha256 = sha256_file(path)
    frame = pl.read_parquet(path)
    if frame.is_empty():
        raise ValueError(f"Bronze file contains no rows: {path}")

    table_name = _table_name(path, root)
    frame = frame.with_columns(
        pl.lit(str(path)).alias("_bronze_file"),
        pl.lit(sha256).alias("_bronze_sha256"),
        pl.lit(CONTRACT_VERSION).alias("_contract_version"),
    )
    identity = BronzeIdentity(
        absolute_path=path,
        sha256=sha256,
        table_name=table_name,
        row_count=frame.height,
        source_timestamp=_source_timestamp(path),
    )
    return frame, identity

