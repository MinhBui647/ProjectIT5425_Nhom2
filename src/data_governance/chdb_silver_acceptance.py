"""Downstream acceptance check that queries governed Silver through chDB."""

from __future__ import annotations

import json
import os
import re
from typing import Any


REQUIRED_SENSOR_COLUMNS = {
    "line_id",
    "uht_temp",
    "_bronze_sha256",
    "_contract_version",
    "_quality_status",
}
_TABLE_NAME = re.compile(r"^[a-z][a-z0-9_]*$")


def _literal(value: str) -> str:
    """Quote a trusted configuration value as a ClickHouse string literal."""
    return "'" + value.replace("\\", "\\\\").replace("'", "\\'") + "'"


def _result_text(result: Any) -> str:
    if hasattr(result, "bytes"):
        payload = result.bytes()
        return payload.decode("utf-8") if isinstance(payload, bytes) else str(payload)
    return str(result)

def _json_each_row(result: Any) -> list[dict[str, Any]]:
    return [
        json.loads(line)
        for line in _result_text(result).splitlines()
        if line.strip()
    ]

def run_sensor_acceptance(
    *,
    endpoint: str,
    bucket: str,
    access_key: str,
    secret_key: str,
    table_name: str = "sensor_telemetry",
) -> dict[str, Any]:
    """Read one MinIO Delta table with chDB and run a downstream smoke query."""
    if not _TABLE_NAME.fullmatch(table_name):
        raise ValueError(f"Invalid Silver table name: {table_name!r}")
    try:
        import chdb
    except ImportError as exc:  # pragma: no cover - exercised in Linux acceptance
        raise RuntimeError(
            "chDB is required; run this acceptance check on Linux/WSL or in Docker"
        ) from exc

    delta_url = (
        f"{endpoint.rstrip('/')}/{bucket}/02_silver_curated/{table_name}/"
    )
    source = (
        "deltaLake("
        f"{_literal(delta_url)}, {_literal(access_key)}, {_literal(secret_key)}"
        ")"
    )
    schema_rows = _json_each_row(
        chdb.query(f"DESCRIBE TABLE {source}", "JSONEachRow")
    )
    columns = {str(row["name"]) for row in schema_rows}
    missing = sorted(REQUIRED_SENSOR_COLUMNS - columns)
    if missing:
        raise AssertionError(f"Silver contract is missing columns: {missing}")

    aggregate_rows = _json_each_row(
        chdb.query(
            "SELECT line_id, count(*) AS row_count, "
            "avg(toFloat64(uht_temp)) AS avg_uht_temp "
            f"FROM {source} GROUP BY line_id ORDER BY line_id",
            "JSONEachRow",
        )
    )
    if not aggregate_rows:
        raise AssertionError("Downstream query returned no sensor rows")
    if sum(int(row["row_count"]) for row in aggregate_rows) <= 0:
        raise AssertionError("Downstream query returned a non-positive row count")
    if any(row.get("avg_uht_temp") is None for row in aggregate_rows):
        raise AssertionError("Downstream query could not calculate avg(uht_temp)")

    return {
        "engine": "chDB (ClickHouse in-process)",
        "table": table_name,
        "delta_url": delta_url,
        "required_columns": sorted(REQUIRED_SENSOR_COLUMNS),
        "schema_column_count": len(columns),
        "query": "count(*) and avg(uht_temp) GROUP BY line_id",
        "groups": aggregate_rows,
    }

def main() -> int:
    result = run_sensor_acceptance(
        endpoint=os.getenv("CHDB_MINIO_ENDPOINT", "http://localhost:9000"),
        bucket=os.getenv("MINIO_BUCKET", "smart-dairy-lakehouse"),
        access_key=os.getenv("MINIO_ACCESS_KEY", "dairy"),
        secret_key=os.getenv("MINIO_SECRET_KEY", "hustit5425"),
        table_name=os.getenv("CHDB_SILVER_TABLE", "sensor_telemetry"),
    )
    print(json.dumps(result, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
