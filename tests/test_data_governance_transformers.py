"""Contract tests for the incremental Bronze transformers."""

from pathlib import Path

import polars as pl
import pyarrow as pa
import pytest
from polars.testing import assert_frame_equal

from data_governance.bronze_loader import load_bronze
from data_governance.transformers import (
    MES_CHEMISTRY_COLUMNS,
    MES_TIMESTAMP_COLUMNS,
    FAO_WIDE_INDEX_COLUMNS,
    SENSOR_MEASUREMENT_COLUMNS,
    transform_mes_lims,
    transform_market_prices,
    transform_market_indices,
    transform_food_recalls,
    transform_bronze,
    transform_sensor_telemetry,
    transform_weather,
)
from lakehouse_storage.schemas import (
    MES_LIMS_SCHEMA,
    MARKET_PRICES_SCHEMA,
    MARKET_INDICES_SCHEMA,
    FOOD_RECALLS_SCHEMA,
    SENSOR_TELEMETRY_SCHEMA,
    WEATHER_SCHEMA,
)


FIXTURE_ROOT = Path(__file__).parent / "fixtures" / "bronze"


def _fixture(table_name: str) -> Path:
    return next((FIXTURE_ROOT / table_name).glob("*.parquet"))


def _arrow_type_matches(actual: pa.DataType, expected: pa.DataType) -> bool:
    if pa.types.is_string(actual) or pa.types.is_large_string(actual):
        return pa.types.is_string(expected) or pa.types.is_large_string(expected)
    return actual == expected


def test_sensor_transform_matches_canonical_business_schema_and_keeps_lineage():
    bronze, _ = load_bronze(_fixture("sensor_telemetry"), FIXTURE_ROOT)
    original = bronze.clone()
    metadata_columns = [name for name in bronze.columns if name.startswith("_")]

    transformed = transform_sensor_telemetry(bronze)

    expected_fields = [
        field for field in SENSOR_TELEMETRY_SCHEMA if field.name != "is_anomaly_flag"
    ]
    actual_schema = transformed.select(
        [field.name for field in expected_fields]
    ).to_arrow().schema
    assert actual_schema.names == [field.name for field in expected_fields]
    assert all(
        _arrow_type_matches(actual.type, expected.type)
        for actual, expected in zip(actual_schema, expected_fields, strict=True)
    )
    assert transformed.schema["timestamp"] == pl.Datetime("us", "UTC")
    assert "is_anomaly_flag" not in transformed.columns
    assert_frame_equal(
        transformed.select(metadata_columns),
        bronze.select(metadata_columns),
    )
    assert_frame_equal(bronze, original)


def test_sensor_transform_uses_float32_and_preserves_nulls():
    row = {
        "timestamp": ["2026-10-15T10:00:00+07:00"],
        "line_id": ["LINE_UHT_1"],
        "batch_id": [None],
        "preheat_temp": [None],
        "uht_temp": [138.5],
        "homo_press_stage1": [200],
        "homo_press_stage2": [30.0],
        "flow_rate": [5000],
        "conductivity": [None],
        "power_kw": [150.0],
        "_source_system": ["TEST_SENSOR"],
        "_bronze_file": ["sensor.parquet"],
    }

    transformed = transform_sensor_telemetry(pl.DataFrame(row))

    assert all(transformed.schema[column] == pl.Float32 for column in SENSOR_MEASUREMENT_COLUMNS)
    assert transformed["preheat_temp"].to_list() == [None]
    assert transformed["conductivity"].to_list() == [None]
    assert transformed["timestamp"].dt.hour().to_list() == [3]
    assert transformed["_bronze_file"].to_list() == ["sensor.parquet"]


def test_sensor_transform_rejects_missing_canonical_column():
    frame = pl.DataFrame({"timestamp": ["2026-10-15T00:00:00Z"]})

    try:
        transform_sensor_telemetry(frame)
    except ValueError as error:
        assert "Missing required columns for sensor_telemetry" in str(error)
        assert "uht_temp" in str(error)
    else:
        raise AssertionError("Expected a missing-column error")


def test_mes_transform_matches_canonical_schema_and_keeps_lineage():
    bronze, _ = load_bronze(_fixture("mes_lims"), FIXTURE_ROOT)
    original = bronze.clone()
    metadata_columns = [name for name in bronze.columns if name.startswith("_")]

    transformed = transform_mes_lims(bronze)

    actual_schema = transformed.select(MES_LIMS_SCHEMA.names).to_arrow().schema
    assert actual_schema.names == MES_LIMS_SCHEMA.names
    assert all(
        _arrow_type_matches(actual.type, expected.type)
        for actual, expected in zip(actual_schema, MES_LIMS_SCHEMA, strict=True)
    )
    assert all(
        transformed.schema[column] == pl.Datetime("us", "UTC")
        for column in MES_TIMESTAMP_COLUMNS
    )
    assert all(
        transformed.schema[column] == pl.Float32
        for column in MES_CHEMISTRY_COLUMNS
    )
    assert transformed.schema["batch_defect_flag"] == pl.Int8
    assert_frame_equal(
        transformed.select(metadata_columns),
        bronze.select(metadata_columns),
    )
    assert_frame_equal(bronze, original)


def test_mes_transform_preserves_nullable_values_and_converts_offset_to_utc():
    row = {
        "batch_id": ["BATCH_001"],
        "line_id": ["LINE_UHT_1"],
        "production_start": ["2026-10-15T10:00:00+07:00"],
        "production_end": [None],
        "sku_id": ["UHT_PURE"],
        "fat_in": [None],
        "protein_in": [3.2],
        "acidity_sh": [6],
        "yield_recovery_pct": [92.5],
        "microbiology_status": [None],
        "batch_defect_flag": [1],
        "_source_system": ["TEST_MES"],
        "_contract_version": ["v1"],
    }

    transformed = transform_mes_lims(pl.DataFrame(row))

    assert transformed["production_start"].dt.hour().to_list() == [3]
    assert transformed["production_end"].to_list() == [None]
    assert transformed["fat_in"].to_list() == [None]
    assert transformed["batch_defect_flag"].to_list() == [1]
    assert transformed["_contract_version"].to_list() == ["v1"]


def test_mes_transform_rejects_missing_canonical_column():
    frame = pl.DataFrame({"batch_id": ["BATCH_001"]})

    try:
        transform_mes_lims(frame)
    except ValueError as error:
        assert "Missing required columns for mes_lims" in str(error)
        assert "production_start" in str(error)
    else:
        raise AssertionError("Expected a missing-column error")


def _weather_frame(temperature, humidity) -> pl.DataFrame:
    return pl.DataFrame(
        {
            "farm_id": ["MOC_CHAU"],
            "observed_at": ["2026-10-15T10:00:00+07:00"],
            "temperature_celsius": [temperature],
            "relative_humidity_pct": [humidity],
            "precipitation_mm": [0.0],
            "_created_at": ["2026-10-15T10:05:00Z"],
            "_bronze_sha256": ["a" * 64],
        }
    )


def test_weather_transform_matches_schema_and_manual_thi():
    bronze, _ = load_bronze(_fixture("weather"), FIXTURE_ROOT)
    transformed = transform_weather(bronze)
    actual_schema = transformed.select(WEATHER_SCHEMA.names).to_arrow().schema

    assert all(
        _arrow_type_matches(actual.type, expected.type)
        for actual, expected in zip(actual_schema, WEATHER_SCHEMA, strict=True)
    )
    manual = transform_weather(_weather_frame(30.0, 80.0))
    expected_thi = (1.8 * 30.0 + 32) - (0.55 - 0.0055 * 80.0) * (
        1.8 * 30.0 - 26
    )
    assert abs(manual["thi_index"][0] - expected_thi) < 1e-5
    assert manual["is_heat_stress"].to_list() == [True]
    assert manual["observed_at"].dt.hour().to_list() == [3]
    assert manual["_bronze_sha256"].to_list() == ["a" * 64]
    assert "temperature_celsius" not in transformed.columns


def test_weather_thi_boundary_and_null_semantics():
    boundary = transform_weather(_weather_frame(22.2222222222, 100.0))
    null_temperature = transform_weather(_weather_frame(None, 80.0))
    null_humidity = transform_weather(_weather_frame(30.0, None))

    assert boundary["thi_index"].to_list() == [72.0]
    assert boundary["is_heat_stress"].to_list() == [False]
    assert null_temperature["thi_index"].to_list() == [None]
    assert null_temperature["is_heat_stress"].to_list() == [None]
    assert null_humidity["thi_index"].to_list() == [None]
    assert null_humidity["is_heat_stress"].to_list() == [None]
    assert boundary.schema["thi_index"] == pl.Float32
    assert boundary.schema["is_heat_stress"] == pl.Boolean


def test_gdt_transform_maps_fixture_and_preserves_metadata():
    bronze, _ = load_bronze(_fixture("market_prices"), FIXTURE_ROOT)
    metadata_columns = [name for name in bronze.columns if name.startswith("_")]

    transformed = transform_market_prices(bronze)
    actual_schema = transformed.select(MARKET_PRICES_SCHEMA.names).to_arrow().schema

    assert all(
        _arrow_type_matches(actual.type, expected.type)
        for actual, expected in zip(actual_schema, MARKET_PRICES_SCHEMA, strict=True)
    )
    assert transformed["source"].unique().to_list() == ["GDT"]
    assert transformed["product"].to_list() == ["AMF", "SMP", "WMP"]
    assert transformed["contract"].unique().to_list() == ["EVENT_AVERAGE"]
    assert transformed["currency"].unique().to_list() == ["USD"]
    assert transformed["unit"].unique().to_list() == ["metric_ton"]
    assert transformed["observed_at"].dt.hour().unique().to_list() == [12]
    assert_frame_equal(
        transformed.select(metadata_columns),
        bronze.select(metadata_columns),
    )


def test_gdt_transform_keeps_invalid_price_as_null_for_validator():
    frame = pl.DataFrame(
        {
            "EventDate": ["2025-01-07T12:00:00Z"],
            "ProductGroupCode": ["WMP"],
            "AveragePublishedPrice": ["not-a-price"],
            "_created_at": ["2026-10-07T08:00:00Z"],
            "_bronze_file": ["gdt.parquet"],
        }
    )

    transformed = transform_market_prices(frame)

    assert transformed["price"].to_list() == [None]
    assert transformed.schema["price"] == pl.Float32
    assert transformed.schema["observed_at"] == pl.Datetime("us", "UTC")
    assert transformed["_bronze_file"].to_list() == ["gdt.parquet"]


def test_fao_transform_accepts_drive_long_fixture():
    bronze, _ = load_bronze(_fixture("market_indices"), FIXTURE_ROOT)
    transformed = transform_market_indices(bronze)
    actual_schema = transformed.select(MARKET_INDICES_SCHEMA.names).to_arrow().schema

    assert transformed.height == bronze.height
    assert all(
        _arrow_type_matches(actual.type, expected.type)
        for actual, expected in zip(actual_schema, MARKET_INDICES_SCHEMA, strict=True)
    )
    assert transformed["source"].unique().to_list() == ["FAO"]
    assert transformed["index_name"].to_list() == ["DAIRY"]
    assert str(transformed["period_start"][0]) == "2025-01-01"
    assert transformed["base_period"].to_list() == ["2014-2016=100"]
    assert transformed["_source_system"].to_list() == ["fao_food_price_index"]


def test_fao_transform_unpivots_six_wide_indices_and_keeps_invalid_as_null():
    wide_data = {
        "Month": ["2025-01", "2025-02-01"],
        "_created_at": ["2026-10-07", "2026-10-07"],
        "_bronze_file": ["fao.parquet", "fao.parquet"],
    }
    for position, column in enumerate(FAO_WIDE_INDEX_COLUMNS):
        wide_data[column] = [str(100 + position), str(110 + position)]
    wide_data["Sugar Price Index"][1] = "not-an-index"
    frame = pl.DataFrame(wide_data)

    transformed = transform_market_indices(frame)

    assert transformed.height == frame.height * len(FAO_WIDE_INDEX_COLUMNS)
    assert set(transformed["index_name"].unique()) == set(FAO_WIDE_INDEX_COLUMNS)
    assert set(str(value) for value in transformed["period_start"].unique()) == {
        "2025-01-01",
        "2025-02-01",
    }
    assert transformed["index_value"].null_count() == 1
    assert transformed.schema["index_value"] == pl.Float32
    assert transformed["base_period"].null_count() == transformed.height
    assert transformed["_bronze_file"].unique().to_list() == ["fao.parquet"]


def test_fda_transform_matches_schema_and_preserves_fixture_identity():
    bronze, _ = load_bronze(_fixture("food_recalls"), FIXTURE_ROOT)
    transformed = transform_food_recalls(bronze)
    actual_schema = transformed.select(FOOD_RECALLS_SCHEMA.names).to_arrow().schema

    assert all(
        _arrow_type_matches(actual.type, expected.type)
        for actual, expected in zip(actual_schema, FOOD_RECALLS_SCHEMA, strict=True)
    )
    assert transformed["source"].unique().to_list() == ["FDA"]
    assert transformed["recall_id"].to_list() == bronze["recall_number"].to_list()
    assert transformed["classification"].to_list() == bronze["classification"].to_list()
    assert transformed["published_at"].dt.hour().unique().to_list() == [0]
    assert transformed["_bronze_sha256"].to_list() == bronze["_bronze_sha256"].to_list()


def test_fda_transform_handles_three_classes_and_empty_optional_fields():
    frame = pl.DataFrame(
        {
            "recall_number": ["FDA-1", "FDA-2", "FDA-3"],
            "report_date": ["20250101", "20250202", "20250303"],
            "classification": ["Class I", "Class II", "Class III"],
            "product_description": ["Milk", "", None],
            "reason_for_recall": [None, "Allergen", ""],
            "status": ["Ongoing", None, "Terminated"],
            "_base_api": ["https://api.fda.gov"] * 3,
            "_created_at": ["2026-10-07"] * 3,
        }
    )

    transformed = transform_food_recalls(frame)

    assert transformed["recall_id"].to_list() == ["FDA-1", "FDA-2", "FDA-3"]
    assert transformed["classification"].to_list() == [
        "Class I",
        "Class II",
        "Class III",
    ]
    assert transformed["product"].to_list() == ["Milk", "", None]
    assert transformed["reason"].to_list() == [None, "Allergen", ""]
    assert transformed["published_at"].dt.day().to_list() == [1, 2, 3]
    assert transformed["source_url"].unique().to_list() == ["https://api.fda.gov"]


@pytest.mark.parametrize(
    "table_name",
    [
        "sensor_telemetry",
        "mes_lims",
        "weather",
        "market_prices",
        "market_indices",
        "food_recalls",
    ],
)
def test_dispatcher_transforms_every_mvp_fixture(table_name):
    bronze, _ = load_bronze(_fixture(table_name), FIXTURE_ROOT)

    transformed = transform_bronze(bronze, table_name)

    assert transformed.height > 0
    assert transformed.schema["ingestion_date"] == pl.Date
    assert transformed["ingestion_date"].null_count() == 0
    required_columns = {
        field.name
        for field in {
            "sensor_telemetry": SENSOR_TELEMETRY_SCHEMA,
            "mes_lims": MES_LIMS_SCHEMA,
            "weather": WEATHER_SCHEMA,
            "market_prices": MARKET_PRICES_SCHEMA,
            "market_indices": MARKET_INDICES_SCHEMA,
            "food_recalls": FOOD_RECALLS_SCHEMA,
        }[table_name]
        if not field.nullable
    }
    assert required_columns.issubset(transformed.columns)


def test_dispatcher_supports_schema_alias_and_rejects_unknown_or_usda():
    sensor, _ = load_bronze(_fixture("sensor_telemetry"), FIXTURE_ROOT)
    assert transform_bronze(sensor, "S1").height == sensor.height

    with pytest.raises(ValueError, match="Unknown governance table"):
        transform_bronze(pl.DataFrame(), "not_a_table")
    with pytest.raises(ValueError, match="Unsupported Silver table 'market_reports'"):
        transform_bronze(pl.DataFrame(), "market_reports")

