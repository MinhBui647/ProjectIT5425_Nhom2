"""Command-line orchestrator for the governed Bronze-to-Silver pipeline."""

from __future__ import annotations

import argparse
import json
import logging
import sys
import uuid
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Any, Sequence

import polars as pl

from data_governance.bronze_loader import load_bronze
from data_governance.config import (
    CONTRACT_VERSION,
    DEFAULT_ERROR_THRESHOLD,
    FIGURES_PATH,
    LINEAGE_SPOOL_PATH,
    QUALITY_REPORTS_PATH,
    QUARANTINE_PATH,
)
from data_governance.contracts import CONTRACTS, validate_table
from data_governance.gatekeeper import apply_gate
from data_governance.gx_context import create_gx_context, run_gx_checkpoint
from data_governance.lineage import LineageEmitter, build_run_event
from data_governance.models import GateStatus
from data_governance.outliers import flag_sensor_outliers
from data_governance.reporting import write_failure_artifact, write_quality_artifacts
from data_governance.silver_writer import already_ingested, write_governed_silver
from data_governance.transformers import transform_bronze
from data_governance.visualization import (
    render_lineage_dag,
    render_missingness_heatmap,
    render_sensor_physical_bands,
)
from lakehouse_storage.config import BRONZE_LOCAL_PATH, get_table_uri
from lakehouse_storage.schemas import normalize_table_name


LOGGER = logging.getLogger(__name__)
MVP_TABLES = tuple(CONTRACTS)
QUALITY_EXCEPTIONS = (FileNotFoundError, ValueError, pl.exceptions.PolarsError)


def _iso_date(value: str) -> date:
    try:
        return date.fromisoformat(value)
    except ValueError as exc:
        raise argparse.ArgumentTypeError(f"Invalid ISO date: {value}") from exc


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Validate Bronze files and commit governed Silver Delta tables."
    )
    parser.add_argument("--table", action="append", default=[], help="One table; repeatable")
    parser.add_argument("--tables", action="append", default=[], help="Comma-separated tables")
    parser.add_argument("--bronze-root", default=BRONZE_LOCAL_PATH)
    parser.add_argument(
        "--silver-root",
        help="Local or s3:// Silver root; defaults to configured MinIO tables",
    )
    parser.add_argument("--date-from", type=_iso_date)
    parser.add_argument("--date-to", type=_iso_date)
    parser.add_argument("--fail-threshold", type=float, default=DEFAULT_ERROR_THRESHOLD)
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--build-data-docs", action="store_true")
    parser.add_argument(
        "--lineage",
        action=argparse.BooleanOptionalAction,
        default=True,
        help="Emit OpenLineage events (default: enabled)",
    )
    parser.add_argument("--lineage-endpoint", default="http://localhost:5000")
    parser.add_argument("--lineage-spool", default=str(LINEAGE_SPOOL_PATH))
    parser.add_argument("--replay-lineage", action="store_true")
    parser.add_argument("--reports-root", default=str(QUALITY_REPORTS_PATH))
    parser.add_argument("--quarantine-root", default=str(QUARANTINE_PATH))
    parser.add_argument("--figures-root", default=str(FIGURES_PATH))
    parser.add_argument("--gx-project-root", default="tmp/gx")
    return parser


def selected_tables(namespace: argparse.Namespace) -> tuple[str, ...]:
    requested = list(namespace.table)
    for value in namespace.tables:
        requested.extend(item.strip() for item in value.split(",") if item.strip())
    requested = requested or list(MVP_TABLES)
    normalized: list[str] = []
    for name in requested:
        table = normalize_table_name(name)
        if table not in MVP_TABLES:
            raise ValueError(f"Unknown governance table: {name}")
        if table not in normalized:
            normalized.append(table)
    return tuple(normalized)


def discover_bronze_files(
    bronze_root: str | Path,
    tables: Sequence[str],
) -> list[Path]:
    """Discover inputs in deterministic table/path order."""
    root = Path(bronze_root).resolve()
    files: list[Path] = []
    for table in tables:
        table_root = root / table
        files.extend(sorted(table_root.rglob("*.parquet"), key=lambda p: p.as_posix()))
    return files


def _silver_uri(table_name: str, silver_root: str | None) -> str:
    if silver_root is None:
        return get_table_uri(table_name)
    root = silver_root.rstrip("/\\")
    if root.startswith("s3://"):
        return f"{root}/{table_name}"
    return str((Path(root) / table_name).resolve())


def _filter_dates(
    frame: pl.DataFrame,
    date_from: date | None,
    date_to: date | None,
) -> pl.DataFrame:
    if date_from and date_to and date_from > date_to:
        raise ValueError("date-from must be on or before date-to")
    filtered = frame
    if date_from:
        filtered = filtered.filter(pl.col("ingestion_date") >= pl.lit(date_from))
    if date_to:
        filtered = filtered.filter(pl.col("ingestion_date") <= pl.lit(date_to))
    return filtered


def _emit(
    emitter: LineageEmitter | None,
    event_type: str,
    *,
    run_id: str,
    table_name: str,
    bronze_path: Path,
    bronze_root: Path,
) -> str | None:
    if emitter is None:
        return None
    event = build_run_event(
        event_type,
        run_id=run_id,
        table_name=table_name,
        bronze_path=bronze_path,
        bronze_root=bronze_root,
    )
    return emitter.emit(event)


def _failure_result(
    *,
    run_id: str,
    table_name: str,
    bronze_path: Path,
    reason: str,
    category: str,
    lineage_status: str | None,
    summary_path: Path | None = None,
) -> dict[str, Any]:
    result = {
        "run_id": run_id,
        "table_name": table_name,
        "bronze_file": str(bronze_path),
        "status": GateStatus.FAIL.value,
        "error_category": category,
        "reason": reason,
        "lineage_status": lineage_status,
        "rows_written": 0,
    }
    if summary_path is not None:
        result["reports"] = {"summary": str(summary_path)}
    return result


def process_bronze_file(
    bronze_path: Path,
    *,
    bronze_root: Path,
    table_name: str,
    silver_root: str | None,
    fail_threshold: float,
    date_from: date | None,
    date_to: date | None,
    dry_run: bool,
    emitter: LineageEmitter | None,
    reports_root: Path,
    quarantine_root: Path,
    figures_root: Path,
    gx_context: Any | None,
) -> dict[str, Any]:
    """Run one immutable Bronze file and return a JSON-compatible result."""
    run_id = str(uuid.uuid4())
    started_at = datetime.now(timezone.utc)
    lineage_states: list[str] = []
    failure_stage = "load_bronze"
    artifact_paths: dict[str, Path] = {}
    start_state = _emit(
        emitter,
        "START",
        run_id=run_id,
        table_name=table_name,
        bronze_path=bronze_path,
        bronze_root=bronze_root,
    )
    if start_state:
        lineage_states.append(start_state)

    try:
        failure_stage = "load_bronze"
        raw, identity = load_bronze(bronze_path, bronze_root)
        failure_stage = "transform_bronze"
        canonical = transform_bronze(raw, table_name)
        failure_stage = "date_filter"
        canonical = _filter_dates(canonical, date_from, date_to)
        if canonical.is_empty():
            complete_state = _emit(
                emitter,
                "COMPLETE",
                run_id=run_id,
                table_name=table_name,
                bronze_path=bronze_path,
                bronze_root=bronze_root,
            )
            if complete_state:
                lineage_states.append(complete_state)
            return {
                "run_id": run_id,
                "table_name": table_name,
                "bronze_file": str(bronze_path),
                "status": "SKIPPED_DATE_RANGE",
                "reason": "No rows matched the requested date range",
                "lineage_status": "SPOOLED" if "SPOOLED" in lineage_states else complete_state,
                "rows_written": 0,
            }
        if table_name == "sensor_telemetry":
            failure_stage = "outlier_detection"
            canonical = flag_sensor_outliers(canonical)
        failure_stage = "quality_validation"
        quality = validate_table(canonical, table_name)
    except QUALITY_EXCEPTIONS as exc:
        try:
            summary_path = write_failure_artifact(
                run_id=run_id,
                table_name=table_name,
                bronze_path=bronze_path,
                failure_stage=failure_stage,
                error_category="quality",
                reason=str(exc),
                started_at=started_at,
                reports_root=reports_root,
            )
        except Exception as audit_exc:
            LOGGER.exception("Could not persist early failure audit for %s", bronze_path)
            summary_path = None
            failure_reason = f"{exc}; failure audit could not be written: {audit_exc}"
            failure_category = "infrastructure"
        else:
            failure_reason = str(exc)
            failure_category = "quality"
        fail_state = _emit(
            emitter,
            "FAIL",
            run_id=run_id,
            table_name=table_name,
            bronze_path=bronze_path,
            bronze_root=bronze_root,
        )
        return _failure_result(
            run_id=run_id,
            table_name=table_name,
            bronze_path=bronze_path,
            reason=failure_reason,
            category=failure_category,
            lineage_status=fail_state,
            summary_path=summary_path,
        )

    table_uri = _silver_uri(table_name, silver_root)
    try:
        failure_stage = "idempotency_check"
        duplicate = already_ingested(table_uri, identity.sha256, CONTRACT_VERSION)
        failure_stage = "quality_gate"
        valid, invalid, decision = apply_gate(
            canonical,
            quality,
            fail_threshold,
            is_duplicate=duplicate,
        )
        failure_stage = "quality_artifacts"
        artifact_paths = write_quality_artifacts(
            invalid,
            run_id=run_id,
            table_name=table_name,
            bronze_identity=identity,
            quality=quality,
            decision=decision,
            started_at=started_at,
            reports_root=reports_root,
            quarantine_root=quarantine_root,
        )
        figure_dir = figures_root / run_id
        failure_stage = "report_figures"
        figure_paths = {
            "missingness_heatmap": render_missingness_heatmap(
                canonical, figure_dir / "missingness_heatmap.png"
            ),
            "lineage_dag": render_lineage_dag(
                {
                    "run_id": run_id,
                    "bronze_file": str(bronze_path),
                    "silver_uri": table_uri,
                    "data_docs": str(artifact_paths["summary"]),
                },
                figure_dir / "lineage_dag.png",
            ),
        }
        if table_name == "sensor_telemetry":
            figure_paths["sensor_physical_bands"] = render_sensor_physical_bands(
                canonical, figure_dir / "sensor_physical_bands.png"
            )

        docs_path = None
        if gx_context is not None:
            failure_stage = "data_docs"
            gx_result = run_gx_checkpoint(gx_context, canonical, table_name)
            docs_path = str(gx_result["data_docs"].get("local_site", ""))

        # Delta is the final mutable operation. If an artifact or Data Docs
        # build fails, the run must fail before Silver can be committed.
        failure_stage = "silver_write"
        write_result = write_governed_silver(
            valid,
            table_uri=table_uri,
            table_name=table_name,
            bronze_identity=identity,
            decision=decision,
            run_id=run_id,
            contract_version=CONTRACT_VERSION,
            dry_run=dry_run,
        )

        terminal_event = (
            "FAIL" if decision.status is GateStatus.FAIL else "COMPLETE"
        )
        terminal_state = _emit(
            emitter,
            terminal_event,
            run_id=run_id,
            table_name=table_name,
            bronze_path=bronze_path,
            bronze_root=bronze_root,
        )
        if terminal_state:
            lineage_states.append(terminal_state)
        return {
            "run_id": run_id,
            "table_name": table_name,
            "bronze_file": str(bronze_path),
            "status": decision.status.value,
            "reason": decision.reason,
            "input_rows": canonical.height,
            "valid_rows": decision.valid_row_count,
            "rejected_rows": decision.rejected_row_count,
            "rows_written": int(write_result["rows_written"]),
            "silver_uri": table_uri,
            "reports": {key: str(value) for key, value in artifact_paths.items()},
            "figures": {key: str(value) for key, value in figure_paths.items()},
            "data_docs": docs_path,
            "lineage_status": "SPOOLED" if "SPOOLED" in lineage_states else terminal_state,
        }
    except Exception as exc:
        summary_path = artifact_paths.get("summary")
        if summary_path is None:
            try:
                summary_path = write_failure_artifact(
                    run_id=run_id,
                    table_name=table_name,
                    bronze_path=bronze_path,
                    failure_stage=failure_stage,
                    error_category="infrastructure",
                    reason=str(exc),
                    started_at=started_at,
                    reports_root=reports_root,
                )
            except Exception:
                LOGGER.exception("Could not persist infrastructure failure audit for %s", bronze_path)
        fail_state = _emit(
            emitter,
            "FAIL",
            run_id=run_id,
            table_name=table_name,
            bronze_path=bronze_path,
            bronze_root=bronze_root,
        )
        LOGGER.exception("Infrastructure failure for %s", bronze_path)
        return _failure_result(
            run_id=run_id,
            table_name=table_name,
            bronze_path=bronze_path,
            reason=str(exc),
            category="infrastructure",
            lineage_status=fail_state,
            summary_path=summary_path,
        )


def run_governance(namespace: argparse.Namespace) -> tuple[dict[str, Any], int]:
    tables = selected_tables(namespace)
    bronze_root = Path(namespace.bronze_root).resolve()
    if not 0 <= namespace.fail_threshold <= 1:
        raise ValueError("fail-threshold must be between 0 and 1")
    if namespace.date_from and namespace.date_to and namespace.date_from > namespace.date_to:
        raise ValueError("date-from must be on or before date-to")
    files = discover_bronze_files(bronze_root, tables)
    if not files:
        raise FileNotFoundError(f"No Bronze Parquet files found below {bronze_root}")

    emitter = (
        LineageEmitter(namespace.lineage_endpoint, spool_path=namespace.lineage_spool)
        if namespace.lineage
        else None
    )
    replay = emitter.replay() if emitter and namespace.replay_lineage else None
    gx = None
    if namespace.build_data_docs:
        gx = create_gx_context(
            project_root=namespace.gx_project_root,
            data_docs_path=Path(namespace.reports_root).parent / "data_docs",
        )

    results: list[dict[str, Any]] = []
    for path in files:
        relative = path.relative_to(bronze_root)
        table_name = normalize_table_name(relative.parts[0])
        results.append(
            process_bronze_file(
                path,
                bronze_root=bronze_root,
                table_name=table_name,
                silver_root=namespace.silver_root,
                fail_threshold=namespace.fail_threshold,
                date_from=namespace.date_from,
                date_to=namespace.date_to,
                dry_run=namespace.dry_run,
                emitter=emitter,
                reports_root=Path(namespace.reports_root),
                quarantine_root=Path(namespace.quarantine_root),
                figures_root=Path(namespace.figures_root),
                gx_context=gx,
            )
        )

    infrastructure_failures = sum(
        result.get("error_category") == "infrastructure" for result in results
    )
    quality_failures = sum(
        result["status"] == GateStatus.FAIL.value
        and result.get("error_category") != "infrastructure"
        for result in results
    )
    exit_code = 3 if infrastructure_failures else (2 if quality_failures else 0)
    summary = {
        "tables": list(tables),
        "files_processed": len(results),
        "quality_failures": quality_failures,
        "infrastructure_failures": infrastructure_failures,
        "dry_run": namespace.dry_run,
        "replay": replay,
        "results": results,
        "exit_code": exit_code,
    }
    return summary, exit_code


def main(argv: Sequence[str] | None = None) -> int:
    parser = build_parser()
    namespace = parser.parse_args(argv)
    try:
        summary, exit_code = run_governance(namespace)
    except (FileNotFoundError, ValueError) as exc:
        summary = {"error": str(exc), "exit_code": 3, "results": []}
        exit_code = 3
    except Exception as exc:
        LOGGER.exception("Unexpected governance CLI failure")
        summary = {"error": str(exc), "exit_code": 3, "results": []}
        exit_code = 3
    print(json.dumps(summary, ensure_ascii=False, default=str))
    return exit_code


if __name__ == "__main__":
    sys.exit(main())
