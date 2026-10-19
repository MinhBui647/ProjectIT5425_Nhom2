"""Governed Silver commits with gate enforcement and checksum idempotency."""

from __future__ import annotations

from pathlib import Path
from typing import Any

import polars as pl
from deltalake import DeltaTable
from deltalake.exceptions import TableNotFoundError

from data_governance.config import CONTRACT_VERSION
from data_governance.models import BronzeIdentity, GateDecision, GateStatus
from lakehouse_storage.config import get_storage_options
from lakehouse_storage.schemas import get_partition_columns, normalize_table_name
from lakehouse_storage.silver_manager import append_to_delta


def already_ingested(
    table_uri: str,
    bronze_sha256: str,
    contract_version: str = CONTRACT_VERSION,
) -> bool:
    """Return whether Silver contains a Bronze digest for one contract version."""
    if not contract_version:
        raise ValueError("contract_version must not be empty")
    if not str(table_uri).startswith("s3://") and not Path(table_uri).exists():
        return False
    try:
        table = DeltaTable(table_uri, storage_options=get_storage_options())
    except TableNotFoundError:
        return False
    schema_names = set(table.schema().to_arrow().names)
    if "_contract_version" not in schema_names:
        raise ValueError(
            "Silver table is missing _contract_version; "
            "versioned idempotency cannot be evaluated"
        )
    if "_bronze_sha256" not in schema_names:
        return False
    match = (
        pl.scan_delta(table_uri, storage_options=get_storage_options())
        .filter(
            (pl.col("_bronze_sha256") == bronze_sha256)
            & (pl.col("_contract_version") == contract_version)
        )
        .select("_bronze_sha256")
        .limit(1)
        .collect()
    )
    return not match.is_empty()


def write_governed_silver(
    valid_df: pl.DataFrame,
    *,
    table_uri: str,
    table_name: str,
    bronze_identity: BronzeIdentity,
    decision: GateDecision,
    run_id: str,
    contract_version: str = CONTRACT_VERSION,
    dry_run: bool = False,
) -> dict[str, Any]:
    """Commit only gate-approved, non-duplicate rows to a Silver Delta table."""
    table_name = normalize_table_name(table_name)
    accepted = {GateStatus.PASS, GateStatus.PASS_WITH_REJECTIONS}
    if decision.status not in accepted:
        return {
            "status": decision.status,
            "rows_written": 0,
            "written": False,
            "reason": "Gate decision does not permit a Silver write",
        }
    if already_ingested(table_uri, bronze_identity.sha256, contract_version):
        return {
            "status": GateStatus.SKIPPED_DUPLICATE,
            "rows_written": 0,
            "written": False,
            "reason": "Bronze checksum already exists in Silver",
        }
    if dry_run:
        return {
            "status": decision.status,
            "rows_written": 0,
            "written": False,
            "reason": "Dry-run: Silver write skipped",
        }
    if valid_df.is_empty():
        return {
            "status": decision.status,
            "rows_written": 0,
            "written": False,
            "reason": "No valid rows to write",
        }

    governed = valid_df.with_columns(
        pl.lit(str(bronze_identity.absolute_path)).alias("_bronze_file"),
        pl.lit(bronze_identity.sha256).alias("_bronze_sha256"),
        pl.lit(decision.status.value).alias("_quality_status"),
        pl.lit(run_id).alias("_quality_run_id"),
        pl.lit(contract_version).alias("_contract_version"),
    )
    append_to_delta(
        governed,
        table_uri,
        partition_by=get_partition_columns(table_name),
        table_name=table_name,
    )
    return {
        "status": decision.status,
        "rows_written": governed.height,
        "written": True,
        "reason": "Silver Delta commit completed",
    }

