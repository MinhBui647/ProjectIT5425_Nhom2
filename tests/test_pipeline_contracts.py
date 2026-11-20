"""Integration boundaries frozen before the multi-source workflow changes."""

import importlib
import inspect

import pyarrow as pa
import pytest

from data_governance.config import CONTRACT_VERSION, DEFAULT_ERROR_THRESHOLD
from data_governance.contracts import CONTRACTS
from data_governance.models import GateStatus
from lakehouse_storage import schemas


PARTITIONS = {
    "sensor_telemetry": ["line_id", "ingestion_date"],
    "mes_lims": ["line_id", "ingestion_date"],
    "market_prices": ["ingestion_date"],
    "market_indices": ["ingestion_date"],
    "weather": ["farm_id", "ingestion_date"],
    "food_recalls": ["ingestion_date"],
}


@pytest.mark.parametrize("table_name,partition", PARTITIONS.items())
def test_required_silver_sources_keep_p3_contract_and_partitions(table_name, partition):
    assert table_name in CONTRACTS
    assert schemas.get_partition_columns(table_name) == partition
    assert "_quality_status" not in schemas.get_silver_schema(table_name).names


@pytest.mark.parametrize(
    "table_name,column,dtype",
    [
        ("sensor_telemetry", "timestamp", pa.string()),
        ("sensor_telemetry", "uht_temp", pa.float64()),
        ("mes_lims", "production_start", pa.string()),
        ("market_prices", "AveragePublishedPrice", pa.string()),
        ("market_indices", "index_value", pa.float64()),
        ("weather", "temperature_celsius", pa.float64()),
        ("weather", "relative_humidity_pct", pa.int64()),
        ("food_recalls", "report_date", pa.string()),
    ],
)
def test_raw_handoff_keeps_source_types(table_name, column, dtype):
    assert schemas.get_bronze_schema(table_name).field(column).type == dtype


def test_fao_keeps_explicit_long_and_wide_variants():
    long, wide = schemas.get_bronze_schema_variants("market_indices")
    assert {"source", "index_name", "period_start", "index_value"} <= set(long.names)
    assert {"Month", "Dairy Price Index", "_created_at"} <= set(wide.names)
    assert wide.field("Dairy Price Index").type == pa.string()


def test_usda_remains_bronze_only_and_metadata_is_supporting_data():
    assert "market_reports" not in schemas.list_tables()
    assert "market_reports" not in CONTRACTS
    with pytest.raises(ValueError, match="stable schema"):
        schemas.get_silver_schema("market_reports")
    assert schemas.get_partition_columns("bronze_metadata") == ["fetched_at"]


def test_quality_contract_keeps_threshold_version_and_outcomes():
    assert CONTRACT_VERSION == "v1"
    assert DEFAULT_ERROR_THRESHOLD == 0.005
    assert {status.value for status in GateStatus} == {
        "PASS", "PASS_WITH_REJECTIONS", "FAIL", "SKIPPED_DUPLICATE",
    }


@pytest.mark.parametrize(
    "module,function,required",
    [
        ("gdt", "get_gdt_marketprice_data", ("start_date", "end_date")),
        ("fao", "get_fao_marketindices_data", ("start_date", "end_date")),
        ("openmeteo", "get_openmeteo_weather_data",
         ("farm_id", "latitude", "longitude", "start_date", "end_date")),
        ("openfda", "get_openfda_foodrecall_data",
         ("start_date", "end_date", "expected_total")),
        ("usda", "get_usda_marketreports_data", ("start_date", "end_date")),
    ],
)
def test_live_collectors_expose_window_without_importing_runner(module, function, required):
    collector = getattr(importlib.import_module(f"crawling_ingestion.{module}"), function)
    signature = inspect.signature(collector)
    assert tuple(
        name for name, param in signature.parameters.items()
        if param.default is inspect.Parameter.empty
    ) == required


def test_p3_callable_keeps_explicit_roots_gate_and_artifact_boundaries():
    from data_governance.run_governance import process_bronze_file

    signature = inspect.signature(process_bronze_file)
    assert tuple(signature.parameters) == (
        "bronze_path", "bronze_root", "table_name", "silver_root", "fail_threshold",
        "date_from", "date_to", "dry_run", "emitter", "reports_root",
        "quarantine_root", "figures_root", "gx_context",
    )
    assert all(
        parameter.kind is inspect.Parameter.KEYWORD_ONLY
        for name, parameter in signature.parameters.items() if name != "bronze_path"
    )
