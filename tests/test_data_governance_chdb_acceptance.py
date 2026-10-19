"""Live downstream Silver acceptance through chDB and MinIO."""

import os
import sys
from types import SimpleNamespace

import pytest

from data_governance.chdb_silver_acceptance import run_sensor_acceptance


def test_chdb_query_checks_schema_and_aggregates(monkeypatch):
    queries = []

    def query(sql, output_format):
        queries.append((sql, output_format))
        if sql.startswith("DESCRIBE TABLE"):
            return "\n".join(
                f'{{"name":"{column}","type":"String"}}'
                for column in (
                    "line_id",
                    "uht_temp",
                    "_bronze_sha256",
                    "_contract_version",
                    "_quality_status",
                )
            )
        return (
            '{"line_id":"LINE_UHT_1","row_count":60,'
            '"avg_uht_temp":139.85}'
        )

    monkeypatch.setitem(sys.modules, "chdb", SimpleNamespace(query=query))
    result = run_sensor_acceptance(
        endpoint="http://minio:9000",
        bucket="smart-dairy-lakehouse",
        access_key="dairy",
        secret_key="demo-secret",
    )

    assert result["schema_column_count"] == 5
    assert result["groups"][0]["row_count"] == 60
    assert result["groups"][0]["avg_uht_temp"] == 139.85
    assert queries[0][0].startswith("DESCRIBE TABLE deltaLake(")
    assert "GROUP BY line_id" in queries[1][0]


@pytest.mark.integration
def test_chdb_consumes_sensor_silver_from_minio():
    if os.getenv("CHDB_SILVER_ACCEPTANCE") != "1":
        pytest.skip("set CHDB_SILVER_ACCEPTANCE=1 and start MinIO to run")

    result = run_sensor_acceptance(
        endpoint=os.getenv("CHDB_MINIO_ENDPOINT", "http://localhost:9000"),
        bucket=os.getenv("MINIO_BUCKET", "smart-dairy-lakehouse"),
        access_key=os.getenv("MINIO_ACCESS_KEY", "dairy"),
        secret_key=os.getenv("MINIO_SECRET_KEY", "hustit5425"),
    )

    assert result["engine"] == "chDB (ClickHouse in-process)"
    assert result["table"] == "sensor_telemetry"
    assert result["schema_column_count"] >= len(result["required_columns"])
    assert sum(int(group["row_count"]) for group in result["groups"]) > 0
    assert all(group["avg_uht_temp"] is not None for group in result["groups"])
