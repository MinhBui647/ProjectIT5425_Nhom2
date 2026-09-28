"""Delta Lake table maintenance - compaction, z-order, and vacuum.

Optimizes Delta tables for query performance and manages storage.
"""

from __future__ import annotations

import logging
from pathlib import Path

import pyarrow as pa
from deltalake import DeltaTable

from lakehouse_storage.config import (
    get_storage_options,
    VACUUM_RETENTION_HOURS_SAFE,
    COMPACTION_MIN_FILE_SIZE_MB,
)

logger = logging.getLogger(__name__)


# =============================================================================
# Compaction
# =============================================================================

def compact_table(
    table_uri: str,
    target_file_size_mb: int = COMPACTION_MIN_FILE_SIZE_MB,
) -> dict:
    """Merge small files into larger ones (bin-packing compaction).

    Args:
        table_uri: Delta table URI
        target_file_size_mb: Target file size in MB (default 128MB)

    Returns:
        Dict with:
        - files_before: number of files before compaction
        - files_after: number of files after compaction
        - files_removed: number of small files merged
        - bytes_removed: bytes freed
    """
    dt = _get_delta_table(table_uri)

    # Get file count before
    files_before = dt.file_uris()
    bytes_before = _table_total_bytes(dt)
    count_before = len(files_before)

    logger.info(
        "Starting compaction for %s (current: %d files, %s)",
        table_uri,
        count_before,
        _format_bytes(bytes_before),
    )

    # Run compaction
    dt.optimize.compact()

    # Get file count after
    dt_after = _get_delta_table(table_uri)
    files_after = dt_after.file_uris()
    bytes_after = _table_total_bytes(dt_after)
    count_after = len(files_after)

    result = {
        "files_before": count_before,
        "files_after": count_after,
        "files_removed": count_before - count_after,
        "bytes_before": bytes_before,
        "bytes_after": bytes_after,
        "bytes_removed": bytes_before - bytes_after,
    }

    logger.info(
        "Compaction complete: %d -> %d files (removed %d, freed %s)",
        count_before,
        count_after,
        result["files_removed"],
        _format_bytes(result["bytes_removed"]),
    )

    return result


def auto_compact_if_needed(
    table_uri: str,
    file_count_threshold: int = 50,
    target_file_size_mb: int = COMPACTION_MIN_FILE_SIZE_MB,
) -> dict | None:
    """Automatically compact if table has too many small files.

    Args:
        table_uri: Delta table URI
        file_count_threshold: Compact if file count exceeds this
        target_file_size_mb: Target file size in MB

    Returns:
        Compaction result dict if compaction was run, None otherwise
    """
    dt = _get_delta_table(table_uri)
    file_count = len(dt.file_uris())

    if file_count > file_count_threshold:
        logger.info(
            "Auto-compact triggered: %d files (threshold: %d)",
            file_count,
            file_count_threshold,
        )
        return compact_table(table_uri, target_file_size_mb)

    logger.debug(
        "Auto-compact skipped: %d files (threshold: %d)",
        file_count,
        file_count_threshold,
    )
    return None


# =============================================================================
# Z-Order Optimization
# =============================================================================

def zorder_table(
    table_uri: str,
    columns: list[str],
) -> dict:
    """Z-order table for better data skipping on specified columns.

    Z-ordering reorganizes data to co-locate related values,
    improving query performance for filtered columns.

    Args:
        table_uri: Delta table URI
        columns: List of columns to Z-order by

    Returns:
        Dict with optimization stats
    """
    if not columns:
        raise ValueError("columns list cannot be empty")

    dt = _get_delta_table(table_uri)

    files_before = len(dt.file_uris())

    # Partition columns cannot be Z-ordered (Delta restriction)
    metadata = dt.metadata()
    partition_cols = set(metadata.partition_columns) if metadata else set()
    zorder_cols = [c for c in columns if c not in partition_cols]

    logger.info(
        "Starting Z-order optimization for %s on columns: %s",
        table_uri,
        zorder_cols,
    )

    # Run Z-order (only non-partition columns)
    if zorder_cols:
        dt.optimize.z_order(zorder_cols)
    else:
        logger.info(
            "Skipping Z-order: all requested columns are partition columns: %s",
            columns,
        )

    # Get stats after
    dt_after = _get_delta_table(table_uri)
    files_after = len(dt_after.file_uris())

    result = {
        "columns": columns,
        "files_before": files_before,
        "files_after": files_after,
        "files_added": files_after - files_before,
    }

    logger.info(
        "Z-order complete: %d -> %d files",
        files_before,
        files_after,
    )

    return result


# =============================================================================
# Vacuum
# =============================================================================

def vacuum_table(
    table_uri: str,
    retention_hours: int = VACUUM_RETENTION_HOURS_SAFE,
    dry_run: bool = True,
    enforce_retention_duration: bool = False,
) -> dict:
    """Remove unused files older than retention period.

    Args:
        table_uri: Delta table URI
        retention_hours: Hours to retain old files (default 168 = 7 days)
        dry_run: If True, only return file list without deleting
        enforce_retention_duration: If True, enforce minimum retention

    Returns:
        Dict with:
        - dry_run: whether this was a dry run
        - files_removed: list of removed/pending removal files
        - bytes_removed: bytes freed
    """
    dt = _get_delta_table(table_uri)

    # Get files before vacuum
    all_files = dt.file_uris()
    total_bytes = _table_total_bytes(dt)
    size_map = _file_size_map(dt)

    logger.info(
        "Starting vacuum for %s (retention: %d hours, dry_run: %s)",
        table_uri,
        retention_hours,
        dry_run,
    )

    # Run vacuum
    removed_files = dt.vacuum(
        retention_hours=retention_hours,
        dry_run=dry_run,
        enforce_retention_duration=enforce_retention_duration,
    )

    # Calculate bytes removed using sizes captured before vacuum
    bytes_removed = sum(_lookup_size(size_map, f) for f in removed_files)

    result = {
        "dry_run": dry_run,
        "retention_hours": retention_hours,
        "files_removed": removed_files,
        "files_count": len(removed_files),
        "bytes_removed": bytes_removed,
        "total_bytes_before": total_bytes,
    }

    if dry_run:
        logger.info(
            "Vacuum dry-run complete: %d files would be removed (freeing %s)",
            len(removed_files),
            _format_bytes(bytes_removed),
        )
    else:
        logger.info(
            "Vacuum complete: %d files removed (freed %s)",
            len(removed_files),
            _format_bytes(bytes_removed),
        )

    return result


def vacuum_all_tables(
    table_uris: list[str],
    retention_hours: int = VACUUM_RETENTION_HOURS_SAFE,
    dry_run: bool = True,
) -> list[dict]:
    """Vacuum multiple tables.

    Args:
        table_uris: List of Delta table URIs
        retention_hours: Hours to retain old files
        dry_run: If True, only return file lists without deleting

    Returns:
        List of vacuum result dicts
    """
    results = []
    for uri in table_uris:
        try:
            result = vacuum_table(uri, retention_hours, dry_run)
            results.append({"table_uri": uri, **result})
        except Exception as e:
            logger.error("Vacuum failed for %s: %s", uri, e)
            results.append({"table_uri": uri, "error": str(e)})

    return results


# =============================================================================
# Table Statistics
# =============================================================================

def get_table_stats(table_uri: str) -> dict:
    """Get comprehensive table statistics.

    Args:
        table_uri: Delta table URI

    Returns:
        Dict with file count, size, version, partition info
    """
    dt = _get_delta_table(table_uri)

    files = dt.file_uris()
    total_bytes = _table_total_bytes(dt)

    # Get partition info if available
    metadata = dt.metadata()
    partition_cols = metadata.partition_columns if metadata else []

    return {
        "table_uri": table_uri,
        "version": dt.version(),
        "file_count": len(files),
        "total_bytes": total_bytes,
        "total_bytes_human": _format_bytes(total_bytes),
        "partition_columns": partition_cols,
        "last_updated": metadata.created_time if metadata else None,
    }


def get_file_sizes(table_uri: str) -> list[dict]:
    """Get detailed file size information.

    Args:
        table_uri: Delta table URI

    Returns:
        List of dicts with file path and size
    """
    dt = _get_delta_table(table_uri)
    actions = pa.table(dt.get_add_actions(flatten=True))
    paths = actions.column("path").to_pylist()
    sizes = actions.column("size_bytes").to_pylist()

    result = []
    for path, size in zip(paths, sizes):
        size = int(size or 0)
        result.append({
            "path": str(path),
            "size_bytes": size,
            "size_human": _format_bytes(size),
        })

    # Sort by size descending
    result.sort(key=lambda x: x["size_bytes"], reverse=True)

    return result


def get_optimization_recommendations(table_uri: str) -> list[str]:
    """Get recommendations for table optimization.

    Args:
        table_uri: Delta table URI

    Returns:
        List of recommendation strings
    """
    recommendations = []

    stats = get_table_stats(table_uri)
    file_count = stats["file_count"]
    total_bytes = stats["total_bytes"]

    # File count recommendations
    if file_count > 100:
        recommendations.append(
            f"High file count ({file_count}). Consider compaction."
        )
    elif file_count > 50:
        recommendations.append(
            f"Moderate file count ({file_count}). Compaction recommended."
        )

    # Average file size recommendations
    if file_count > 0:
        avg_size = total_bytes / file_count
        if avg_size < 50 * 1024 * 1024:  # < 50MB
            recommendations.append(
                f"Small average file size ({_format_bytes(avg_size)}). "
                "Compaction would reduce file count."
            )

    # Z-order recommendations
    if "line_id" not in stats["partition_columns"]:
        recommendations.append(
            "Consider Z-ordering on 'line_id' or 'ingestion_date' "
            "for better query performance."
        )

    if not recommendations:
        recommendations.append("Table appears well-optimized.")

    return recommendations


# =============================================================================
# Maintenance Schedule
# =============================================================================

def run_full_maintenance(
    table_uri: str,
    compact: bool = True,
    zorder_cols: list[str] | None = None,
    vacuum: bool = True,
    vacuum_retention_hours: int = VACUUM_RETENTION_HOURS_SAFE,
    vacuum_dry_run: bool = True,
) -> dict:
    """Run full maintenance cycle on a table.

    Order: compaction -> z-order -> vacuum

    Args:
        table_uri: Delta table URI
        compact: Whether to run compaction
        zorder_cols: Columns for Z-order (or None to skip)
        vacuum: Whether to run vacuum
        vacuum_retention_hours: Retention period for vacuum
        vacuum_dry_run: If True (default), vacuum only reports files without
            deleting. Pass False explicitly to perform a destructive vacuum.

    Returns:
        Dict with results of each step
    """
    results = {"table_uri": table_uri}

    logger.info("Starting full maintenance for %s", table_uri)

    # Compaction
    if compact:
        try:
            results["compaction"] = compact_table(table_uri)
        except Exception as e:
            logger.error("Compaction failed: %s", e)
            results["compaction"] = {"error": str(e)}

    # Z-order
    if zorder_cols:
        try:
            results["zorder"] = zorder_table(table_uri, zorder_cols)
        except Exception as e:
            logger.error("Z-order failed: %s", e)
            results["zorder"] = {"error": str(e)}

    # Vacuum
    if vacuum:
        try:
            results["vacuum"] = vacuum_table(
                table_uri,
                retention_hours=vacuum_retention_hours,
                dry_run=vacuum_dry_run,
            )
        except Exception as e:
            logger.error("Vacuum failed: %s", e)
            results["vacuum"] = {"error": str(e)}

    logger.info("Full maintenance complete for %s", table_uri)
    return results


# =============================================================================
# Helper Functions
# =============================================================================

def _get_delta_table(table_uri: str) -> "DeltaTable":
    """Get DeltaTable object with storage options."""
    return DeltaTable(table_uri, storage_options=get_storage_options())


def _table_total_bytes(dt: "DeltaTable") -> int:
    """Total on-disk bytes from Delta metadata (works for local and S3)."""
    actions = pa.table(dt.get_add_actions(flatten=True))
    return sum(int(s or 0) for s in actions.column("size_bytes").to_pylist())


def _file_size_map(dt: "DeltaTable") -> dict[str, int]:
    """Map data-file references to sizes using Delta add actions.

    Keys include the relative path, its basename, and the full table URI path
    so both metadata paths and vacuum-returned paths resolve.
    """
    actions = pa.table(dt.get_add_actions(flatten=True))
    paths = actions.column("path").to_pylist()
    sizes = actions.column("size_bytes").to_pylist()

    base = str(dt.table_uri).rstrip("/")
    mapping: dict[str, int] = {}
    for rel, size in zip(paths, sizes):
        size = int(size or 0)
        rel = str(rel)
        mapping[rel] = size
        mapping[Path(rel).name] = size
        mapping[f"{base}/{rel}"] = size
    return mapping


def _lookup_size(size_map: dict[str, int], path) -> int:
    """Resolve a file size from a size map, tolerating absolute/local forms."""
    path = str(path)
    if path in size_map:
        return size_map[path]
    return size_map.get(Path(path).name, 0)


def _format_bytes(bytes_count: int) -> str:
    """Format bytes as human-readable string."""
    if bytes_count == 0:
        return "0 B"

    units = ["B", "KB", "MB", "GB", "TB"]
    unit_idx = 0
    size = float(bytes_count)

    while size >= 1024 and unit_idx < len(units) - 1:
        size /= 1024
        unit_idx += 1

    return f"{size:.2f} {units[unit_idx]}"
