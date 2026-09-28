"""Tests for schema definitions."""

import pytest
import pyarrow as pa
import polars as pl

from lakehouse_storage.schemas import (
    # New schema names
    SENSOR_TELEMETRY_SCHEMA,
    BRONZE_SENSOR_TELEMETRY_SCHEMA,
    MES_LIMS_SCHEMA,
    BRONZE_MES_LIMS_SCHEMA,
    MARKET_PRICES_SCHEMA,
    BRONZE_MARKET_PRICES_SCHEMA,
    MARKET_INDICES_SCHEMA,
    BRONZE_MARKET_INDICES_SCHEMA,
    WEATHER_SCHEMA,
    BRONZE_WEATHER_SCHEMA,
    FOOD_RECALLS_SCHEMA,
    BRONZE_FOOD_RECALLS_SCHEMA,
    # Legacy aliases
    BRONZE_S1_SCHEMA,
    SILVER_S1_SCHEMA,
    BRONZE_S2_SCHEMA,
    SILVER_S2_SCHEMA,
    # Registry
    SCHEMA_REGISTRY,
    SCHEMA_ALIASES,
    list_tables,
    get_bronze_schema,
    get_silver_schema,
    get_partition_columns,
    get_source_name,
    get_silver_table_uri,
    get_id_column,
    # Constants
    FARM_LOCATIONS,
    THI_THRESHOLD_STRESS,
    VALID_SKU_CODES,
    VALID_MICROBIOLOGY_STATUS,
)


class TestSchemaRegistry:
    """Test schema registry and lookup functions."""

    def test_list_tables(self):
        """All tables are registered."""
        tables = list_tables()
        expected = [
            "sensor_telemetry", "mes_lims", "market_prices", "market_indices",
            "weather", "food_recalls", "bronze_metadata"
        ]
        assert set(tables) == set(expected)

    def test_schema_aliases_s1_to_s5(self):
        """S1-S5 aliases map to new table names."""
        assert SCHEMA_ALIASES["S1"] == "sensor_telemetry"
        assert SCHEMA_ALIASES["S2"] == "mes_lims"
        assert SCHEMA_ALIASES["S3"] == "market_prices"
        assert SCHEMA_ALIASES["S4"] == "weather"
        assert SCHEMA_ALIASES["S5"] == "food_recalls"

    def test_get_bronze_schema_valid_tables(self):
        """All table names return correct Bronze schemas."""
        for table in list_tables():
            schema = get_bronze_schema(table)
            assert isinstance(schema, pa.Schema)

    def test_get_silver_schema_valid_tables(self):
        """All table names return correct Silver schemas."""
        for table in list_tables():
            schema = get_silver_schema(table)
            assert isinstance(schema, pa.Schema)

    def test_get_bronze_schema_with_alias(self):
        """S1-S5 aliases work for schema lookup."""
        schema = get_bronze_schema("S1")
        assert isinstance(schema, pa.Schema)
        assert "timestamp" in schema.names

    def test_get_bronze_schema_invalid_table(self):
        """Invalid table name raises ValueError."""
        with pytest.raises(ValueError, match="Unknown table"):
            get_bronze_schema("invalid_table")

    def test_get_partition_columns(self):
        """Partition columns are correctly defined per table."""
        # sensor_telemetry and mes_lims have 2 partition columns
        assert len(get_partition_columns("sensor_telemetry")) == 2
        assert len(get_partition_columns("mes_lims")) == 2
        # market_prices, market_indices, food_recalls have 1 partition column
        assert len(get_partition_columns("market_prices")) == 1
        assert len(get_partition_columns("market_indices")) == 1
        assert len(get_partition_columns("food_recalls")) == 1
        # weather has 2 partition columns
        assert len(get_partition_columns("weather")) == 2

    def test_get_source_name(self):
        """Source names are correctly mapped."""
        assert get_source_name("sensor_telemetry") == "sensor_telemetry"
        assert get_source_name("mes_lims") == "mes_lims"
        assert get_source_name("weather") == "weather"
        assert get_source_name("food_recalls") == "food_recalls"

    def test_get_silver_table_uri(self):
        """Silver table URIs are correctly formatted."""
        uri = get_silver_table_uri("sensor_telemetry")
        assert uri.startswith("s3://")
        assert "02_silver_curated" in uri
        assert "sensor_telemetry" in uri

    def test_get_id_column(self):
        """ID columns are correctly defined."""
        assert get_id_column("sensor_telemetry") == "timestamp"
        assert get_id_column("mes_lims") == "batch_id"
        assert get_id_column("weather") == "observed_at"
        assert get_id_column("food_recalls") == "recall_id"


class TestSensorTelemetrySchema:
    """Test sensor_telemetry schemas."""

    def test_bronze_schema_fields(self):
        """Bronze sensor_telemetry has all required fields."""
        names = BRONZE_SENSOR_TELEMETRY_SCHEMA.names
        required = ["timestamp", "line_id", "uht_temp", "_source_system"]
        for field in required:
            assert field in names, f"Missing field: {field}"

    def test_bronze_uht_temp_not_nullable(self):
        """Bronze sensor_telemetry: uht_temp is NOT nullable."""
        uht_field = BRONZE_SENSOR_TELEMETRY_SCHEMA.field("uht_temp")
        assert not uht_field.nullable

    def test_bronze_timestamp_is_string(self):
        """Bronze sensor_telemetry: timestamp is string (raw)."""
        ts_field = BRONZE_SENSOR_TELEMETRY_SCHEMA.field("timestamp")
        assert ts_field.type == pa.string()

    def test_bronze_measures_are_float64(self):
        """Bronze sensor_telemetry: all measures are float64."""
        measure_fields = [
            "preheat_temp", "uht_temp", "homo_press_stage1",
            "homo_press_stage2", "flow_rate", "conductivity", "power_kw"
        ]
        for field_name in measure_fields:
            field = BRONZE_SENSOR_TELEMETRY_SCHEMA.field(field_name)
            assert field.type == pa.float64(), f"{field_name} should be float64"

    def test_silver_timestamp_is_timestamp(self):
        """Silver sensor_telemetry: timestamp is timestamp with timezone."""
        ts_field = SENSOR_TELEMETRY_SCHEMA.field("timestamp")
        assert pa.types.is_timestamp(ts_field.type)

    def test_silver_measures_are_float32(self):
        """Silver sensor_telemetry: all measures are float32."""
        measure_fields = [
            "preheat_temp", "uht_temp", "homo_press_stage1",
            "homo_press_stage2", "flow_rate", "conductivity", "power_kw"
        ]
        for field_name in measure_fields:
            field = SENSOR_TELEMETRY_SCHEMA.field(field_name)
            assert field.type == pa.float32(), f"{field_name} should be float32"

    def test_silver_batch_id_nullable(self):
        """Silver sensor_telemetry: batch_id is nullable."""
        batch_id = SENSOR_TELEMETRY_SCHEMA.field("batch_id")
        assert batch_id.nullable


class TestMesLimsSchema:
    """Test mes_lims schemas."""

    def test_bronze_schema_fields(self):
        """Bronze mes_lims has all required fields."""
        names = BRONZE_MES_LIMS_SCHEMA.names
        required = ["batch_id", "line_id", "sku_id", "batch_defect_flag"]
        for field in required:
            assert field in names

    def test_bronze_batch_defect_flag_is_int64(self):
        """Bronze mes_lims: batch_defect_flag is int64."""
        field = BRONZE_MES_LIMS_SCHEMA.field("batch_defect_flag")
        assert field.type == pa.int64()

    def test_silver_production_times_are_timestamp(self):
        """Silver mes_lims: production times are timestamps."""
        start = MES_LIMS_SCHEMA.field("production_start")
        end = MES_LIMS_SCHEMA.field("production_end")
        assert pa.types.is_timestamp(start.type)
        assert pa.types.is_timestamp(end.type)

    def test_valid_sku_codes(self):
        """Valid SKU codes are defined."""
        assert "UHT_PURE" in VALID_SKU_CODES
        assert "UHT_SWEETENED" in VALID_SKU_CODES
        assert "UHT_LOWFAT" in VALID_SKU_CODES

    def test_valid_microbiology_status(self):
        """Valid microbiology status values are defined."""
        assert "PASSED" in VALID_MICROBIOLOGY_STATUS
        assert "FAILED" in VALID_MICROBIOLOGY_STATUS


class TestMarketPricesSchema:
    """Test market_prices schemas."""

    def test_bronze_schema_fields(self):
        """Bronze market_prices has all required fields."""
        names = BRONZE_MARKET_PRICES_SCHEMA.names
        required = ["source", "product", "observed_at", "price", "currency"]
        for field in required:
            assert field in names

    def test_bronze_price_is_float64(self):
        """Bronze market_prices: price is float64."""
        price_field = BRONZE_MARKET_PRICES_SCHEMA.field("price")
        assert price_field.type == pa.float64()

    def test_silver_price_is_float32(self):
        """Silver market_prices: price is float32."""
        price_field = MARKET_PRICES_SCHEMA.field("price")
        assert price_field.type == pa.float32()


class TestMarketIndicesSchema:
    """Test market_indices schemas."""

    def test_bronze_schema_fields(self):
        """Bronze market_indices has all required fields."""
        names = BRONZE_MARKET_INDICES_SCHEMA.names
        required = ["source", "index_name", "period_start", "index_value"]
        for field in required:
            assert field in names

    def test_silver_period_start_is_date(self):
        """Silver market_indices: period_start is date."""
        field = MARKET_INDICES_SCHEMA.field("period_start")
        assert pa.types.is_date(field.type)


class TestWeatherSchema:
    """Test weather schemas."""

    def test_bronze_schema_fields(self):
        """Bronze weather has all required fields."""
        names = BRONZE_WEATHER_SCHEMA.names
        required = ["farm_id", "observed_at", "temperature_c", "relative_humidity_pct"]
        for field in required:
            assert field in names

    def test_thi_threshold_defined(self):
        """THI stress threshold is defined."""
        assert THI_THRESHOLD_STRESS == 72.0

    def test_farm_locations_defined(self):
        """All 5 farm locations are defined."""
        assert len(FARM_LOCATIONS) == 5
        assert "MOC_CHAU" in FARM_LOCATIONS
        assert "BA_VI" in FARM_LOCATIONS
        assert "NGHE_AN" in FARM_LOCATIONS
        assert "LAM_DONG" in FARM_LOCATIONS
        assert "CU_CHI" in FARM_LOCATIONS

    def test_farm_coordinates(self):
        """Farm coordinates are valid lat/lon."""
        for farm_code, location in FARM_LOCATIONS.items():
            lat = location["lat"]
            lon = location["lon"]
            assert -90 <= lat <= 90, f"{farm_code}: invalid latitude {lat}"
            assert -180 <= lon <= 180, f"{farm_code}: invalid longitude {lon}"


class TestFoodRecallsSchema:
    """Test food_recalls schemas."""

    def test_bronze_schema_fields(self):
        """Bronze food_recalls has all required fields."""
        names = BRONZE_FOOD_RECALLS_SCHEMA.names
        required = ["source", "recall_id", "published_at"]
        for field in required:
            assert field in names

    def test_silver_published_at_is_timestamp(self):
        """Silver food_recalls: published_at is timestamp."""
        field = FOOD_RECALLS_SCHEMA.field("published_at")
        assert pa.types.is_timestamp(field.type)


class TestBronzeMetadataSchema:
    """Test bronze_metadata schemas."""

    def test_bronze_metadata_fields(self):
        """Bronze metadata has all required fields."""
        from lakehouse_storage.schemas import BRONZE_METADATA_RAW_SCHEMA
        names = BRONZE_METADATA_RAW_SCHEMA.names
        required = ["source", "url", "fetched_at", "sha256", "file"]
        for field in required:
            assert field in names


class TestSchemaTypeConversion:
    """Test that Bronze to Silver type conversions are correct."""

    def test_bronze_to_silver_type_mapping_float(self):
        """Type mapping: Bronze float64 -> Silver float32."""
        # Sensor telemetry measures
        bronze_uht = BRONZE_SENSOR_TELEMETRY_SCHEMA.field("uht_temp")
        silver_uht = SENSOR_TELEMETRY_SCHEMA.field("uht_temp")
        assert bronze_uht.type == pa.float64()
        assert silver_uht.type == pa.float32()

    def test_bronze_to_silver_timestamp_mapping(self):
        """Type mapping: Bronze string -> Silver timestamp."""
        bronze_ts = BRONZE_SENSOR_TELEMETRY_SCHEMA.field("timestamp")
        silver_ts = SENSOR_TELEMETRY_SCHEMA.field("timestamp")
        assert bronze_ts.type == pa.string()
        assert pa.types.is_timestamp(silver_ts.type)


class TestLegacyAliases:
    """Test backward compatibility with S1-S5 naming."""

    def test_bronze_s1_alias_matches_new(self):
        """BRONZE_S1_SCHEMA == BRONZE_SENSOR_TELEMETRY_SCHEMA."""
        assert BRONZE_S1_SCHEMA.equals(BRONZE_SENSOR_TELEMETRY_SCHEMA)

    def test_silver_s1_alias_matches_new(self):
        """SILVER_S1_SCHEMA == SENSOR_TELEMETRY_SCHEMA."""
        assert SILVER_S1_SCHEMA.equals(SENSOR_TELEMETRY_SCHEMA)

    def test_bronze_s2_alias_matches_new(self):
        """BRONZE_S2_SCHEMA == BRONZE_MES_LIMS_SCHEMA."""
        assert BRONZE_S2_SCHEMA.equals(BRONZE_MES_LIMS_SCHEMA)


class TestSchemaValidationWithPolars:
    """Test schema validation using Polars."""

    def test_create_dataframe_with_bronze_sensor_schema(self):
        """Can create Polars DataFrame matching Bronze sensor schema."""
        import datetime

        df = pl.DataFrame({
            "timestamp": ["2026-01-15T10:00:00"],
            "line_id": ["LINE_UHT_1"],
            "batch_id": [None],
            "preheat_temp": [75.0],
            "uht_temp": [138.5],
            "homo_press_stage1": [200.0],
            "homo_press_stage2": [30.0],
            "flow_rate": [5000.0],
            "conductivity": [4.5],
            "power_kw": [150.0],
            "_source_system": ["TEST"],
            "_generated_at": ["2026-01-15T10:00:00"],
        })

        # Verify all required columns exist
        assert set(df.columns) == set(BRONZE_SENSOR_TELEMETRY_SCHEMA.names)

    def test_create_dataframe_with_silver_sensor_schema(self):
        """Can create Polars DataFrame matching Silver sensor schema."""
        from datetime import datetime, date

        df = pl.DataFrame({
            "timestamp": [datetime(2026, 1, 15, 10, 0, 0)],
            "line_id": ["LINE_UHT_1"],
            "batch_id": [None],
            "preheat_temp": [75.0],
            "uht_temp": [138.5],
            "homo_press_stage1": [200.0],
            "homo_press_stage2": [30.0],
            "flow_rate": [5000.0],
            "conductivity": [4.5],
            "power_kw": [150.0],
        })

        # Verify column types
        assert df.schema["timestamp"] == pl.Datetime
        assert df.schema["uht_temp"] == pl.Float64  # Polars uses float64 by default


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
