"""Smart Dairy Factory Lakehouse Storage.

Public API for lakehouse_storage package.

Modules:
- config: Configuration and storage options
- schemas: PyArrow schemas for all data sources (Source of Truth)
- bronze_writer: Write immutable raw data to Bronze layer
- silver_manager: Delta Lake ACID writes to Silver layer
- time_travel: Historical queries for ISO 22000 traceability
- maintenance: Table optimization and cleanup

Tables:
- sensor_telemetry: IoT sensor readings (1 sample/second)
- mes_lims: Production batch logs and quality tests
- market_prices: International commodity prices (GDT/CME/USDA)
- market_indices: FAO dairy price indices
- weather: Farm weather data with THI
- food_recalls: Food safety alerts and recalls (FDA/VFA)
- bronze_metadata: Raw data collection metadata
"""

from lakehouse_storage import config
from lakehouse_storage.schemas import (
    # Schemas
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
    BRONZE_METADATA_SCHEMA,
    BRONZE_METADATA_RAW_SCHEMA,
    # Registry
    SCHEMA_REGISTRY,
    SCHEMA_ALIASES,
    get_bronze_schema,
    get_silver_schema,
    get_partition_columns,
    get_source_name,
    get_table_name,
    get_silver_table_uri,
    get_id_column,
    list_tables,
    get_table_description,
    # Constants
    TIMEZONE,
    LINEAGE_COLUMNS,
    PARTITION_COLUMNS,
    FARM_LOCATIONS,
    THI_THRESHOLD_STRESS,
    VALID_SKU_CODES,
    VALID_SHIFT_CODES,
    VALID_SEVERITY_LEVELS,
    VALID_HAZARD_TYPES,
    VALID_MICROBIOLOGY_STATUS,
    VALID_GDT_PRODUCTS,
    VALID_CME_PRODUCTS,
)

from lakehouse_storage.bronze_writer import (
    write_bronze_batch,
    verify_checksum,
    list_bronze_files,
    read_bronze_file,
    ingest_multiple_files,
)

from lakehouse_storage.silver_manager import (
    append_to_delta,
    ingest_bronze_file,
    read_silver_table,
    export_silver_parquet,
    get_delta_table,
    get_table_version,
    get_table_stats,
    delete_from_delta,
    create_delta_table,
)

from lakehouse_storage.time_travel import (
    load_version,
    get_table_history,
    load_as_of,
    diff_versions,
    trace_batch,
    trace_event,
    trace_batch_history,
    generate_audit_report,
    get_latest_version,
    get_version_at_date,
)

from lakehouse_storage.maintenance import (
    compact_table,
    auto_compact_if_needed,
    zorder_table,
    vacuum_table,
    vacuum_all_tables,
    get_table_stats as get_maintenance_stats,
    get_file_sizes,
    get_optimization_recommendations,
    run_full_maintenance,
)

# Data generators
from lakehouse_storage.mock_sensor_stream import (
    generate_sensor_batch,
    write_sensor_batch,
)
from lakehouse_storage.generate_mes_data import (
    generate_mes_batch,
    generate_daily_production,
    write_mes_batch,
)

__all__ = [
    # Config
    "config",
    # Schemas
    "SENSOR_TELEMETRY_SCHEMA",
    "BRONZE_SENSOR_TELEMETRY_SCHEMA",
    "MES_LIMS_SCHEMA",
    "BRONZE_MES_LIMS_SCHEMA",
    "MARKET_PRICES_SCHEMA",
    "BRONZE_MARKET_PRICES_SCHEMA",
    "MARKET_INDICES_SCHEMA",
    "BRONZE_MARKET_INDICES_SCHEMA",
    "WEATHER_SCHEMA",
    "BRONZE_WEATHER_SCHEMA",
    "FOOD_RECALLS_SCHEMA",
    "BRONZE_FOOD_RECALLS_SCHEMA",
    "BRONZE_METADATA_SCHEMA",
    "BRONZE_METADATA_RAW_SCHEMA",
    # Registry
    "SCHEMA_REGISTRY",
    "SCHEMA_ALIASES",
    "get_bronze_schema",
    "get_silver_schema",
    "get_partition_columns",
    "get_source_name",
    "get_table_name",
    "get_silver_table_uri",
    "get_id_column",
    "list_tables",
    "get_table_description",
    # Constants
    "TIMEZONE",
    "LINEAGE_COLUMNS",
    "PARTITION_COLUMNS",
    "FARM_LOCATIONS",
    "THI_THRESHOLD_STRESS",
    "VALID_SKU_CODES",
    "VALID_SHIFT_CODES",
    "VALID_SEVERITY_LEVELS",
    "VALID_HAZARD_TYPES",
    "VALID_MICROBIOLOGY_STATUS",
    "VALID_GDT_PRODUCTS",
    "VALID_CME_PRODUCTS",
    # Bronze Writer
    "write_bronze_batch",
    "verify_checksum",
    "list_bronze_files",
    "read_bronze_file",
    "ingest_multiple_files",
    # Silver Manager
    "append_to_delta",
    "ingest_bronze_file",
    "read_silver_table",
    "export_silver_parquet",
    "get_delta_table",
    "get_table_version",
    "get_table_stats",
    "delete_from_delta",
    "create_delta_table",
    # Time Travel
    "load_version",
    "get_table_history",
    "load_as_of",
    "diff_versions",
    "trace_batch",
    "trace_event",
    "trace_batch_history",
    "generate_audit_report",
    "get_latest_version",
    "get_version_at_date",
    # Maintenance
    "compact_table",
    "auto_compact_if_needed",
    "zorder_table",
    "vacuum_table",
    "vacuum_all_tables",
    "get_maintenance_stats",
    "get_file_sizes",
    "get_optimization_recommendations",
    "run_full_maintenance",
    # Data generators
    "generate_sensor_batch",
    "write_sensor_batch",
    "generate_mes_batch",
    "generate_daily_production",
    "write_mes_batch",
]

__all__.extend([
    "BRONZE_S1_SCHEMA",
    "SILVER_S1_SCHEMA",
    "BRONZE_S2_SCHEMA",
    "SILVER_S2_SCHEMA",
    "BRONZE_S3_SCHEMA",
    "SILVER_S3_SCHEMA",
    "BRONZE_S4_SCHEMA",
    "SILVER_S4_SCHEMA",
    "BRONZE_S5_SCHEMA",
    "SILVER_S5_SCHEMA",
])

BRONZE_S1_SCHEMA = BRONZE_SENSOR_TELEMETRY_SCHEMA
SILVER_S1_SCHEMA = SENSOR_TELEMETRY_SCHEMA
BRONZE_S2_SCHEMA = BRONZE_MES_LIMS_SCHEMA
SILVER_S2_SCHEMA = MES_LIMS_SCHEMA
BRONZE_S3_SCHEMA = BRONZE_MARKET_PRICES_SCHEMA
SILVER_S3_SCHEMA = MARKET_PRICES_SCHEMA
BRONZE_S4_SCHEMA = BRONZE_WEATHER_SCHEMA
SILVER_S4_SCHEMA = WEATHER_SCHEMA
BRONZE_S5_SCHEMA = BRONZE_FOOD_RECALLS_SCHEMA
SILVER_S5_SCHEMA = FOOD_RECALLS_SCHEMA

__version__ = "2.0.0"
