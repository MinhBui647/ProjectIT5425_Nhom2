"""Bronze layer writer - immutable raw data landing."""

import logging
from datetime import date, datetime, timezone
from pathlib import Path

import polars as pl

from lakehouse_storage.config import BRONZE_LOCAL_PATH
from lakehouse_storage.schemas import (
    get_bronze_schema,
    get_source_name,
    normalize_table_name,
)
from lakehouse_storage.utils import sha256_file

logger = logging.getLogger(__name__)

# Columns to use for date extraction per table
DATE_COLUMNS = {
    "sensor_telemetry": ["timestamp"],
    "mes_lims": ["production_start"],
    "market_prices": ["EventDate"],
    "market_reports": ["published_date"],
    "market_indices": ["Month"],
    "weather": ["observed_at"],
    "food_recalls": ["report_date"],
    "bronze_metadata": ["fetched_at"],
}


def write_bronze_batch(
    df: pl.DataFrame,
    table_name: str,
    bronze_root: str | Path | None = None,
    validate_schema: bool = True,
) -> str:
    """Write DataFrame as Parquet + SHA-256 sidecar to Bronze layer.

    Args:
        df: Data to write
        table_name: Table name (sensor_telemetry, mes_lims, etc.)
        bronze_root: Output directory (default: data/01_bronze_vault)
        validate_schema: Check required columns against Bronze schema

    Returns:
        Path to written Parquet file
    """
    if bronze_root is None:
        bronze_root = Path(BRONZE_LOCAL_PATH)
    else:
        bronze_root = Path(bronze_root)

    if validate_schema:
        _validate_schema(df, table_name)

    partition_date = _extract_date(df, table_name)
    source_name = get_source_name(table_name)
    out_dir = bronze_root / source_name / partition_date.isoformat()
    out_dir.mkdir(parents=True, exist_ok=True)

    timestamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S_%f")
    filename = f"{source_name}_{timestamp}.parquet"
    filepath = out_dir / filename

    if filepath.exists():
        raise FileExistsError(f"File exists (append-only): {filepath}")

    df.write_parquet(filepath)
    _write_checksum(filepath)

    logger.info("Bronze write: %s (%d rows)", filepath, len(df))
    return str(filepath)


def verify_checksum(bronze_path: str | Path) -> bool:
    """Verify SHA-256 sidecar matches file content."""
    bronze_path = Path(bronze_path)
    if not bronze_path.suffix == ".parquet":
        bronze_path = bronze_path.with_suffix(".parquet")

    if not bronze_path.exists():
        raise FileNotFoundError(f"File not found: {bronze_path}")

    sidecar = bronze_path.with_suffix(".sha256")
    if not sidecar.exists():
        logger.warning("No checksum sidecar: %s", sidecar)
        return False

    expected = sidecar.read_text().strip()
    actual = sha256_file(bronze_path)
    return expected == actual


def list_bronze_files(
    table_name: str,
    bronze_root: str | Path | None = None,
    date_from: date | str | None = None,
    date_to: date | str | None = None,
) -> list[Path]:
    """List all Bronze files for a table within date range."""
    if bronze_root is None:
        bronze_root = Path(BRONZE_LOCAL_PATH)
    else:
        bronze_root = Path(bronze_root)

    source_name = get_source_name(table_name)
    source_dir = bronze_root / source_name

    if not source_dir.exists():
        return []

    all_files = list(source_dir.rglob("*.parquet"))

    if date_from is not None or date_to is not None:
        date_from = date.fromisoformat(date_from) if isinstance(date_from, str) else date_from
        date_to = date.fromisoformat(date_to) if isinstance(date_to, str) else date_to

        filtered = []
        for f in all_files:
            try:
                file_date = date.fromisoformat(f.parent.name)
                if date_from and file_date < date_from:
                    continue
                if date_to and file_date > date_to:
                    continue
                filtered.append(f)
            except ValueError:
                filtered.append(f)
        return sorted(filtered)

    return sorted(all_files)


def read_bronze_file(
    bronze_path: str | Path,
    bronze_root: str | Path | None = None,
) -> pl.DataFrame:
    """Read a Bronze Parquet file.

    When ``bronze_root`` is provided, the path is validated to stay within it.
    """
    if bronze_root is not None:
        bronze_path = ensure_within_bronze(bronze_path, bronze_root)
    return pl.read_parquet(bronze_path)


def ensure_within_bronze(
    path: str | Path,
    bronze_root: str | Path | None = None,
) -> Path:
    """Resolve a local Bronze path and ensure it stays within the vault root.

    Raises ``ValueError`` on path traversal. ``s3://`` URIs are returned
    unchanged because they are resolved by the object store, not the FS.
    """
    raw = str(path)
    if raw.startswith("s3://"):
        return Path(raw)

    resolved = Path(path).resolve()
    root = Path(bronze_root if bronze_root is not None else BRONZE_LOCAL_PATH).resolve()
    try:
        resolved.relative_to(root)
    except ValueError as e:
        raise ValueError(
            f"Path traversal detected: {resolved} is outside Bronze root {root}"
        ) from e
    return resolved


def _validate_schema(df: pl.DataFrame, table_name: str) -> None:
    """Validate required columns match Bronze schema."""
    expected = get_bronze_schema(table_name)
    expected_names = set(expected.names)
    actual_names = set(df.columns)

    missing_required = []
    for col in expected_names - actual_names:
        field = expected.field(col)
        if not field.nullable:
            missing_required.append(col)

    if missing_required:
        raise ValueError(f"Missing required columns for {table_name}: {missing_required}")


def _write_checksum(path: Path) -> None:
    """Write SHA-256 sidecar file."""
    checksum = sha256_file(path)
    sidecar = path.with_suffix(".sha256")
    sidecar.write_text(checksum)


def _extract_date(df: pl.DataFrame, table_name: str) -> date:
    """Extract partition date from DataFrame."""
    if df.is_empty():
        raise ValueError(f"Cannot write empty batch to Bronze for table {table_name}")

    table_name = normalize_table_name(table_name)

    date_cols = DATE_COLUMNS.get(table_name, ["timestamp"])

    for date_col in date_cols:
        if date_col not in df.columns:
            continue

        non_null = df.select(pl.col(date_col).drop_nulls()).to_series()
        if len(non_null) == 0:
            continue

        first = non_null[0]

        if isinstance(first, str):
            try:
                # Try ISO format first
                return datetime.fromisoformat(first.replace("Z", "+00:00")).date()
            except ValueError:
                try:
                    # Try common date formats
                    for fmt in ["%Y-%m-%d", "%d/%m/%Y", "%m/%d/%Y"]:
                        return datetime.strptime(first[:10], fmt).date()
                except ValueError:
                    continue
        elif isinstance(first, datetime):
            return first.date()
        elif isinstance(first, date):
            return first

    return datetime.now(timezone.utc).date()


def ingest_multiple_files(
    table_name: str,
    bronze_paths: list[str | Path],
    bronze_root: str | Path | None = None,
) -> list[dict]:
    """Verify a batch of Bronze files and report checksums.

    Each file is validated individually: paths outside ``bronze_root`` or the
    table's source directory are captured as per-file errors so one bad entry
    does not abort the whole batch.
    """
    source_name = get_source_name(table_name)
    root = Path(bronze_root) if bronze_root is not None else None
    expected_dir = (root / source_name) if root is not None else None

    results = []
    for path in bronze_paths:
        path = Path(path)
        try:
            if root is not None:
                path = ensure_within_bronze(path, root)
            if expected_dir is not None and expected_dir not in path.parents:
                raise ValueError(
                    f"File does not belong to table '{table_name}': {path}"
                )
            checksum = sha256_file(path)
            verified = verify_checksum(path)
            results.append({
                "path": str(path),
                "checksum": checksum,
                "verified": verified,
                "size_bytes": path.stat().st_size,
            })
        except Exception as e:
            results.append({"path": str(path), "error": str(e), "verified": False})
    return results
