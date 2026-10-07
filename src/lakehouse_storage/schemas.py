"""PyArrow schemas for all data sources - Source of Truth.

This module defines schemas following the Smart Dairy Factory architecture.
All schema definitions are centralized here and used by all other modules.

Tables:
    - sensor_telemetry: IoT sensor readings (1 sample/second)
    - mes_lims: Production batch logs and quality tests
    - market_prices: International dairy commodity prices (GDT/CME/USDA)
    - market_indices: FAO dairy price indices
    - weather: Farm weather data with THI stress index
    - food_recalls: Food safety alerts and recalls (FDA/VFA)
    - bronze_metadata: Raw data collection metadata for traceability
"""

import pyarrow as pa

from lakehouse_storage.config import TIMEZONE

# =============================================================================
# Configuration
# =============================================================================

# Columns added during Bronze -> Silver transformation
LINEAGE_COLUMNS = [
    "_source_system",
    "_bronze_file",
    "_bronze_sha256",
    "_ingested_at",
    "ingestion_date",
]

# Partition strategy for Silver layer (Delta Lake)
PARTITION_COLUMNS = {
    "sensor_telemetry": ["line_id", "ingestion_date"],
    "mes_lims": ["line_id", "ingestion_date"],
    "market_prices": ["ingestion_date"],
    "market_indices": ["ingestion_date"],
    "weather": ["farm_id", "ingestion_date"],
    "food_recalls": ["ingestion_date"],
    "bronze_metadata": ["fetched_at"],
}

# Valid codes for data validation
VALID_SKU_CODES = ["UHT_PURE", "UHT_SWEETENED", "UHT_LOWFAT"]
VALID_SHIFT_CODES = ["MORNING", "AFTERNOON", "NIGHT"]
VALID_GDT_PRODUCTS = ["WMP", "SMP", "AMF", "BUTTER", "CHEDDAR"]
VALID_CME_PRODUCTS = ["CLASSIII", "CLASSIV", "BOT", "SMP_FUT", "WMP_FUT"]
VALID_SEVERITY_LEVELS = ["Class I", "Class II", "Class III"]
VALID_HAZARD_TYPES = ["Microbiological", "Foreign Matter", "Chemical", "Allergen", "Physical"]
VALID_MICROBIOLOGY_STATUS = ["PASSED", "FAILED", "PENDING"]

# Farm locations for weather data
FARM_LOCATIONS = {
    "MOC_CHAU": {"lat": 20.84, "lon": 104.63, "name": "Moc Chau"},
    "BA_VI": {"lat": 21.08, "lon": 105.37, "name": "Ba Vi"},
    "NGHE_AN": {"lat": 18.67, "lon": 105.68, "name": "Nghe An"},
    "LAM_DONG": {"lat": 11.94, "lon": 108.45, "name": "Lam Dong"},
    "CU_CHI": {"lat": 10.89, "lon": 106.51, "name": "Cu Chi"},
}

THI_THRESHOLD_STRESS = 72.0

# =============================================================================
# Table 1: sensor_telemetry (IoT Sensor Readings)
# =============================================================================

SENSOR_TELEMETRY_SCHEMA = pa.schema([
    pa.field("timestamp", pa.timestamp("us", tz=TIMEZONE), nullable=False),
    pa.field("line_id", pa.string(), nullable=False),
    pa.field("batch_id", pa.string(), nullable=True),
    pa.field("preheat_temp", pa.float32(), nullable=True),
    pa.field("uht_temp", pa.float32(), nullable=False),
    pa.field("homo_press_stage1", pa.float32(), nullable=True),
    pa.field("homo_press_stage2", pa.float32(), nullable=True),
    pa.field("flow_rate", pa.float32(), nullable=True),
    pa.field("conductivity", pa.float32(), nullable=True),
    pa.field("power_kw", pa.float32(), nullable=True),
])

# Bronze version: raw data with string timestamps
BRONZE_SENSOR_TELEMETRY_SCHEMA = pa.schema([
    pa.field("timestamp", pa.string(), nullable=False),
    pa.field("line_id", pa.string(), nullable=False),
    pa.field("batch_id", pa.string(), nullable=True),
    pa.field("preheat_temp", pa.float64(), nullable=True),
    pa.field("uht_temp", pa.float64(), nullable=False),
    pa.field("homo_press_stage1", pa.float64(), nullable=True),
    pa.field("homo_press_stage2", pa.float64(), nullable=True),
    pa.field("flow_rate", pa.float64(), nullable=True),
    pa.field("conductivity", pa.float64(), nullable=True),
    pa.field("power_kw", pa.float64(), nullable=True),
    pa.field("_source_system", pa.string(), nullable=False),
    pa.field("_generated_at", pa.string(), nullable=False),
])

# =============================================================================
# Table 2: mes_lims (Production Batch & Quality Tests)
# =============================================================================

MES_LIMS_SCHEMA = pa.schema([
    pa.field("batch_id", pa.string(), nullable=False),
    pa.field("line_id", pa.string(), nullable=False),
    pa.field("production_start", pa.timestamp("us", tz=TIMEZONE), nullable=True),
    pa.field("production_end", pa.timestamp("us", tz=TIMEZONE), nullable=True),
    pa.field("sku_id", pa.string(), nullable=False),
    pa.field("fat_in", pa.float32(), nullable=True),
    pa.field("protein_in", pa.float32(), nullable=True),
    pa.field("acidity_sh", pa.float32(), nullable=True),
    pa.field("yield_recovery_pct", pa.float32(), nullable=True),
    pa.field("microbiology_status", pa.string(), nullable=True),
    pa.field("batch_defect_flag", pa.int8(), nullable=False),
])

# Bronze version
BRONZE_MES_LIMS_SCHEMA = pa.schema([
    pa.field("batch_id", pa.string(), nullable=False),
    pa.field("line_id", pa.string(), nullable=False),
    pa.field("production_start", pa.string(), nullable=True),
    pa.field("production_end", pa.string(), nullable=True),
    pa.field("sku_id", pa.string(), nullable=False),
    pa.field("fat_in", pa.float64(), nullable=True),
    pa.field("protein_in", pa.float64(), nullable=True),
    pa.field("acidity_sh", pa.float64(), nullable=True),
    pa.field("yield_recovery_pct", pa.float64(), nullable=True),
    pa.field("microbiology_status", pa.string(), nullable=True),
    pa.field("batch_defect_flag", pa.int64(), nullable=False),
    pa.field("_source_system", pa.string(), nullable=False),
    pa.field("_generated_at", pa.string(), nullable=False),
])

# =============================================================================
# Table 3: market_prices (International Commodity Prices)
# =============================================================================

MARKET_PRICES_SCHEMA = pa.schema([
    pa.field("source", pa.string(), nullable=False),
    pa.field("product", pa.string(), nullable=False),
    pa.field("contract", pa.string(), nullable=True),
    pa.field("observed_at", pa.timestamp("us", tz=TIMEZONE), nullable=False),
    pa.field("price", pa.float32(), nullable=True),
    pa.field("currency", pa.string(), nullable=False),
    pa.field("unit", pa.string(), nullable=True),
    pa.field("ingested_at", pa.timestamp("us", tz=TIMEZONE), nullable=False),
])

# Bronze version
BRONZE_MARKET_PRICES_SCHEMA = pa.schema([
    pa.field("EventNumber", pa.string(), nullable=False),
    pa.field("EventDate", pa.string(), nullable=False),
    pa.field("ProductGroupGUID", pa.string(), nullable=False),
    pa.field("ProductGroupName", pa.string(), nullable=False),
    pa.field("PriceIndexPercentageChange", pa.string(), nullable=False),
    pa.field("AveragePublishedPrice", pa.string(), nullable=False),

    pa.field("ProductGroupCode", pa.string(), nullable=False),
    pa.field("_created_at", pa.string(), nullable=False),
])

# =============================================================================
# Table 4: market_indices (FAO Dairy Price Indices)
# =============================================================================

MARKET_INDICES_SCHEMA = pa.schema([
    pa.field("source", pa.string(), nullable=False),
    pa.field("index_name", pa.string(), nullable=False),
    pa.field("period_start", pa.date32(), nullable=False),
    pa.field("index_value", pa.float32(), nullable=True),
    pa.field("base_period", pa.string(), nullable=True),
    pa.field("ingested_at", pa.timestamp("us", tz=TIMEZONE), nullable=False),
])

# Bronze version
BRONZE_MARKET_INDICES_SCHEMA = pa.schema([
    pa.field("source", pa.string(), nullable=False),
    pa.field("index_name", pa.string(), nullable=False),
    pa.field("period_start", pa.string(), nullable=False),
    pa.field("index_value", pa.float64(), nullable=True),
    pa.field("base_period", pa.string(), nullable=True),
    pa.field("ingested_at", pa.string(), nullable=False),
    pa.field("_source_system", pa.string(), nullable=False),
    pa.field("_generated_at", pa.string(), nullable=False),
])

# =============================================================================
# Table 5: weather (Farm Weather + THI)
# =============================================================================

WEATHER_SCHEMA = pa.schema([
    pa.field("farm_id", pa.string(), nullable=False),
    pa.field("observed_at", pa.timestamp("us", tz=TIMEZONE), nullable=False),
    pa.field("temperature_c", pa.float32(), nullable=True),
    pa.field("relative_humidity_pct", pa.float32(), nullable=True),
    pa.field("precipitation_mm", pa.float32(), nullable=True),
    pa.field("ingested_at", pa.timestamp("us", tz=TIMEZONE), nullable=False),
])

# Bronze version
BRONZE_WEATHER_SCHEMA = pa.schema([
    pa.field("farm_id", pa.string(), nullable=False),
    pa.field("observed_at", pa.string(), nullable=False),
    pa.field("temperature_celsius", pa.float64(), nullable=True),
    pa.field("relative_humidity_pct", pa.float64(), nullable=True),
    pa.field("precipitation_mm", pa.float64(), nullable=True),
    pa.field("_created_at", pa.string(), nullable=False),
])

# =============================================================================
# Table 6: food_recalls (Food Safety Alerts & Recalls)
# =============================================================================

FOOD_RECALLS_SCHEMA = pa.schema([
    pa.field("source", pa.string(), nullable=False),
    pa.field("recall_id", pa.string(), nullable=False),
    pa.field("published_at", pa.timestamp("us", tz=TIMEZONE), nullable=False),
    pa.field("product", pa.string(), nullable=True),
    pa.field("reason", pa.string(), nullable=True),
    pa.field("status", pa.string(), nullable=True),
    pa.field("source_url", pa.string(), nullable=True),
    pa.field("ingested_at", pa.timestamp("us", tz=TIMEZONE), nullable=False),
])

# Bronze version
BRONZE_FOOD_RECALLS_SCHEMA = pa.schema([
    pa.field("status", pa.string(), nullable=False),
    pa.field("city", pa.string(), nullable=False),
    pa.field("state", pa.string(), nullable=False),
    pa.field("country", pa.string(), nullable=False),
    pa.field("classification", pa.string(), nullable=False),
    pa.field("openfda", pa.string(), nullable=False),
    pa.field("product_type", pa.string(), nullable=False),
    pa.field("event_id", pa.string(), nullable=False),
    pa.field("recalling_firm", pa.string(), nullable=False),
    pa.field("address_1", pa.string(), nullable=False),
    pa.field("address_2", pa.string(), nullable=False),
    pa.field("postal_code", pa.string(), nullable=False),
    pa.field("voluntary_mandated", pa.string(), nullable=False),
    pa.field("initial_firm_notification", pa.string(), nullable=False),
    pa.field("distribution_pattern", pa.string(), nullable=False),
    pa.field("recall_number", pa.string(), nullable=False),
    pa.field("product_description", pa.string(), nullable=False),
    pa.field("product_quantity", pa.string(), nullable=False),
    pa.field("reason_for_recall", pa.string(), nullable=False),
    pa.field("recall_initiation_date", pa.string(), nullable=False),
    pa.field("center_classification_date", pa.string(), nullable=False),
    pa.field("termination_date", pa.string(), nullable=False),
    pa.field("report_date", pa.string(), nullable=False),
    pa.field("code_info", pa.string(), nullable=False),
    pa.field("more_code_info", pa.string(), nullable=False),
    pa.field("meta_last_updated", pa.string(), nullable=False),
    pa.field("meta_results_skip", pa.string(), nullable=False),
    pa.field("meta_results_limit", pa.string(), nullable=False),
    pa.field("meta_results_total", pa.string(), nullable=False),
    pa.field("_expected_total", pa.string(), nullable=False),
    pa.field("_created_at", pa.string(), nullable=False),
])

# =============================================================================
# Table 7: bronze_metadata (Bronze Layer Metadata)
# =============================================================================

BRONZE_METADATA_SCHEMA = pa.schema([
    pa.field("source", pa.string(), nullable=False),
    pa.field("url", pa.string(), nullable=True),
    pa.field("final_url", pa.string(), nullable=True),
    pa.field("fetched_at", pa.timestamp("us", tz=TIMEZONE), nullable=False),
    pa.field("sha256", pa.string(), nullable=False),
    pa.field("bytes", pa.int64(), nullable=True),
    pa.field("content_type", pa.string(), nullable=True),
    pa.field("file", pa.string(), nullable=False),
])

# Bronze metadata in raw landing zone
BRONZE_METADATA_RAW_SCHEMA = pa.schema([
    pa.field("source", pa.string(), nullable=False),
    pa.field("url", pa.string(), nullable=True),
    pa.field("final_url", pa.string(), nullable=True),
    pa.field("fetched_at", pa.string(), nullable=False),
    pa.field("sha256", pa.string(), nullable=False),
    pa.field("bytes", pa.int64(), nullable=True),
    pa.field("content_type", pa.string(), nullable=True),
    pa.field("file", pa.string(), nullable=False),
    pa.field("_source_system", pa.string(), nullable=False),
    pa.field("_generated_at", pa.string(), nullable=False),
])

# =============================================================================
# Schema Registry
# =============================================================================

BRONZE_USDA_MARKET_PRICES_SCHEMA = pa.schema([
    pa.field("source", pa.string(), nullable=False),
    pa.field("product", pa.string(), nullable=False),
    pa.field("contract", pa.string(), nullable=True),
    pa.field("observed_at", pa.string(), nullable=False),
    pa.field("price", pa.float64(), nullable=True),
    pa.field("currency", pa.string(), nullable=False),
    pa.field("unit", pa.string(), nullable=True),
    pa.field("ingested_at", pa.string(), nullable=False),
    pa.field("_source_system", pa.string(), nullable=False),
    pa.field("_generated_at", pa.string(), nullable=False),
])

SCHEMA_REGISTRY = {
    "usda_market_prices": {
        "source_name": "usda_market_prices",
        "table_name": "market_prices",
        "bronze": BRONZE_USDA_MARKET_PRICES_SCHEMA,
        "silver": MARKET_PRICES_SCHEMA,
        "partition": PARTITION_COLUMNS["market_prices"],
        "id_column": "observed_at",
        "description": "USDA NDPSR weekly prices (USD/lb), separate raw schema from GDT",
    },
    "sensor_telemetry": {
        "source_name": "sensor_telemetry",
        "table_name": "sensor_telemetry",
        "bronze": BRONZE_SENSOR_TELEMETRY_SCHEMA,
        "silver": SENSOR_TELEMETRY_SCHEMA,
        "partition": PARTITION_COLUMNS["sensor_telemetry"],
        "id_column": "timestamp",
        "description": "IoT sensor readings from production line",
    },
    "mes_lims": {
        "source_name": "mes_lims",
        "table_name": "mes_lims",
        "bronze": BRONZE_MES_LIMS_SCHEMA,
        "silver": MES_LIMS_SCHEMA,
        "partition": PARTITION_COLUMNS["mes_lims"],
        "id_column": "batch_id",
        "description": "Production batch logs and LIMS quality tests",
    },
    "market_prices": {
        "source_name": "market_prices",
        "table_name": "market_prices",
        "bronze": BRONZE_MARKET_PRICES_SCHEMA,
        "silver": MARKET_PRICES_SCHEMA,
        "partition": PARTITION_COLUMNS["market_prices"],
        "id_column": "observed_at",
        "description": "International dairy commodity prices (GDT/CME/USDA)",
    },
    "market_indices": {
        "source_name": "market_indices",
        "table_name": "market_indices",
        "bronze": BRONZE_MARKET_INDICES_SCHEMA,
        "silver": MARKET_INDICES_SCHEMA,
        "partition": PARTITION_COLUMNS["market_indices"],
        "id_column": "period_start",
        "description": "FAO dairy price indices",
    },
    "weather": {
        "source_name": "weather",
        "table_name": "weather",
        "bronze": BRONZE_WEATHER_SCHEMA,
        "silver": WEATHER_SCHEMA,
        "partition": PARTITION_COLUMNS["weather"],
        "id_column": "observed_at",
        "description": "Farm weather data with THI stress index",
    },
    "food_recalls": {
        "source_name": "food_recalls",
        "table_name": "food_recalls",
        "bronze": BRONZE_FOOD_RECALLS_SCHEMA,
        "silver": FOOD_RECALLS_SCHEMA,
        "partition": PARTITION_COLUMNS["food_recalls"],
        "id_column": "recall_id",
        "description": "Food safety alerts and recalls (FDA/VFA)",
    },
    "bronze_metadata": {
        "source_name": "bronze_metadata",
        "table_name": "bronze_metadata",
        "bronze": BRONZE_METADATA_RAW_SCHEMA,
        "silver": BRONZE_METADATA_SCHEMA,
        "partition": PARTITION_COLUMNS["bronze_metadata"],
        "id_column": "file",
        "description": "Bronze layer metadata for traceability",
    },
}

SCHEMA_ALIASES = {
    "S1": "sensor_telemetry",
    "S2": "mes_lims",
    "S3": "market_prices",
    "S4": "weather",
    "S5": "food_recalls",
}


def normalize_table_name(table_name: str) -> str:
    """Resolve S1-S5 aliases (case-insensitive) to a canonical table name.

    Non-alias names are lowercased and normalized to snake_case.
    """
    raw = str(table_name).strip()
    alias = SCHEMA_ALIASES.get(raw.upper())
    if alias is not None:
        return alias
    return raw.lower().replace("-", "_").replace(" ", "_")

# =============================================================================
# Utility Functions
# =============================================================================

def get_bronze_schema(table_name: str) -> pa.Schema:
    """Get Bronze schema for a table."""
    table_name = normalize_table_name(table_name)
    if table_name not in SCHEMA_REGISTRY:
        raise ValueError(f"Unknown table: {table_name}")
    return SCHEMA_REGISTRY[table_name]["bronze"]


def get_silver_schema(table_name: str) -> pa.Schema:
    """Get Silver schema for a table."""
    table_name = normalize_table_name(table_name)
    if table_name not in SCHEMA_REGISTRY:
        raise ValueError(f"Unknown table: {table_name}")
    return SCHEMA_REGISTRY[table_name]["silver"]


def get_partition_columns(table_name: str) -> list:
    """Get partition columns for a table."""
    table_name = normalize_table_name(table_name)
    if table_name not in SCHEMA_REGISTRY:
        raise ValueError(f"Unknown table: {table_name}")
    return SCHEMA_REGISTRY[table_name]["partition"]


def get_source_name(table_name: str) -> str:
    """Get Bronze source folder name for a table."""
    table_name = normalize_table_name(table_name)
    if table_name not in SCHEMA_REGISTRY:
        raise ValueError(f"Unknown table: {table_name}")
    return SCHEMA_REGISTRY[table_name]["source_name"]


def get_table_name(table_name: str) -> str:
    """Get Delta table name for a table."""
    table_name = normalize_table_name(table_name)
    if table_name not in SCHEMA_REGISTRY:
        raise ValueError(f"Unknown table: {table_name}")
    return SCHEMA_REGISTRY[table_name]["table_name"]


def get_id_column(table_name: str) -> str:
    """Get primary ID column for a table."""
    table_name = normalize_table_name(table_name)
    if table_name not in SCHEMA_REGISTRY:
        raise ValueError(f"Unknown table: {table_name}")
    return SCHEMA_REGISTRY[table_name]["id_column"]


def get_silver_table_uri(
    table_name: str,
    bucket: str | None = None
) -> str:
    """Get S3 URI for Silver Delta table.

    Uses ``MINIO_BUCKET`` from config when no bucket override is given.
    """
    from lakehouse_storage.config import MINIO_BUCKET

    table_name = normalize_table_name(table_name)
    table = get_table_name(table_name)
    return f"s3://{bucket or MINIO_BUCKET}/02_silver_curated/{table}"


def list_tables() -> list[str]:
    """List all table names."""
    return list(SCHEMA_REGISTRY.keys())


def get_table_description(table_name: str) -> str:
    """Get table description."""
    table_name = normalize_table_name(table_name)
    if table_name not in SCHEMA_REGISTRY:
        raise ValueError(f"Unknown table: {table_name}")
    return SCHEMA_REGISTRY[table_name].get("description", "")


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
