"""PyArrow schemas for all data sources - Source of Truth.

This module defines schemas following the Smart Dairy Factory architecture.
All schema definitions are centralized here and used by all other modules.

Tables:
    - sensor_telemetry: IoT sensor readings (1 sample/second)
    - mes_lims: Production batch logs and quality tests
    - market_prices: Global Dairy Trade (GDT) event prices
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

# Sources that may be retained in Bronze but do not have a stable Silver v1
# contract. They are deliberately kept out of SCHEMA_REGISTRY/list_tables().
UNSUPPORTED_SILVER_TABLES = {
    "market_reports": "USDA report sections do not share a stable schema",
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


# GDT (market_prices) product codes
GDT_PRODUCT_CODES = ["AMF", "SMP", "WMP"]

# USDA keyword to filter reports
USDA_REPORT_KEYWORD = "dairy"


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
    pa.field("is_anomaly_flag", pa.bool_(), nullable=True),
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
# Table 3: market_prices (GDT)
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
    pa.field("ProductGroupGUID", pa.string(), nullable=True),
    pa.field("ProductGroupName", pa.string(), nullable=True),
    pa.field("PriceIndexPercentageChange", pa.string(), nullable=True),
    pa.field("AveragePublishedPrice", pa.string(), nullable=False),
    pa.field("ProductGroupCode", pa.string(), nullable=False),
    # Older handed-off Bronze files predate this crawler metadata column.
    pa.field("_base_api", pa.string(), nullable=True),
    pa.field("_created_at", pa.string(), nullable=False),
])

# =============================================================================
# Table 4: market_indices (FAO)
# =============================================================================

MARKET_INDICES_SCHEMA = pa.schema([
    pa.field("source", pa.string(), nullable=False),
    pa.field("index_name", pa.string(), nullable=False),
    pa.field("period_start", pa.date32(), nullable=False),
    pa.field("index_value", pa.float32(), nullable=True),
    pa.field("base_period", pa.string(), nullable=True),
    pa.field("ingested_at", pa.timestamp("us", tz=TIMEZONE), nullable=False),
])

# Bronze handoff contract. The TV2 Drive vault stores FAO in long form; this is
# intentionally distinct from the older wide crawler draft documented earlier.
BRONZE_MARKET_INDICES_SCHEMA = pa.schema([
    pa.field("source", pa.string(), nullable=False),
    pa.field("index_name", pa.string(), nullable=False),
    pa.field("period_start", pa.string(), nullable=False),
    pa.field("index_value", pa.float64(), nullable=False),
    pa.field("base_period", pa.string(), nullable=True),
    pa.field("ingested_at", pa.string(), nullable=True),
    pa.field("_source_system", pa.string(), nullable=True),
    pa.field("_generated_at", pa.string(), nullable=True),
    pa.field("_created_at", pa.string(), nullable=True),
    pa.field("_source_url", pa.string(), nullable=True),
    pa.field("_raw_record", pa.string(), nullable=True),
    pa.field("_value_status", pa.string(), nullable=True),
])

# The crawler currently emits an older wide FAO shape. Keep it as an explicit
# accepted Bronze variant until TV2 and the handed-off vault converge.
BRONZE_MARKET_INDICES_WIDE_SCHEMA = pa.schema([
    pa.field("Month", pa.string(), nullable=False),
    pa.field("Food Price Index", pa.string(), nullable=False),
    pa.field("Meat Price Index", pa.string(), nullable=False),
    pa.field("Dairy Price Index", pa.string(), nullable=False),
    pa.field("Cereals Price Index", pa.string(), nullable=False),
    pa.field("Oils Price Index", pa.string(), nullable=False),
    pa.field("Sugar Price Index", pa.string(), nullable=False),
    pa.field("_base_api", pa.string(), nullable=True),
    pa.field("_created_at", pa.string(), nullable=False),
])

# =============================================================================
# Table 5: weather (OpenMeteo)
# =============================================================================

WEATHER_SCHEMA = pa.schema([
    pa.field("farm_id", pa.string(), nullable=False),
    pa.field("observed_at", pa.timestamp("us", tz=TIMEZONE), nullable=False),
    pa.field("temperature_c", pa.float32(), nullable=True),
    pa.field("relative_humidity_pct", pa.float32(), nullable=True),
    pa.field("precipitation_mm", pa.float32(), nullable=True),
    pa.field("thi_index", pa.float32(), nullable=True),
    pa.field("is_heat_stress", pa.bool_(), nullable=True),
    pa.field("ingested_at", pa.timestamp("us", tz=TIMEZONE), nullable=False),
])

# Bronze version
BRONZE_WEATHER_SCHEMA = pa.schema([
    pa.field("farm_id", pa.string(), nullable=False),
    pa.field("observed_at", pa.string(), nullable=False),
    pa.field("temperature_celsius", pa.float64(), nullable=True),
    pa.field("relative_humidity_pct", pa.int64(), nullable=True),
    pa.field("precipitation_mm", pa.float64(), nullable=True),
    pa.field("_base_api", pa.string(), nullable=True),
    pa.field("_created_at", pa.string(), nullable=False),
])

# =============================================================================
# Table 6: food_recalls (OpenFDA)
# =============================================================================

FOOD_RECALLS_SCHEMA = pa.schema([
    pa.field("source", pa.string(), nullable=False),
    pa.field("recall_id", pa.string(), nullable=False),
    pa.field("published_at", pa.timestamp("us", tz=TIMEZONE), nullable=False),
    pa.field("product", pa.string(), nullable=True),
    pa.field("reason", pa.string(), nullable=True),
    pa.field("status", pa.string(), nullable=True),
    pa.field("classification", pa.string(), nullable=False),
    pa.field("source_url", pa.string(), nullable=True),
    pa.field("ingested_at", pa.timestamp("us", tz=TIMEZONE), nullable=False),
])

# Bronze version
BRONZE_FOOD_RECALLS_SCHEMA = pa.schema([
    pa.field("status", pa.string(), nullable=True),
    pa.field("city", pa.string(), nullable=True),
    pa.field("state", pa.string(), nullable=True),
    pa.field("country", pa.string(), nullable=True),
    pa.field("classification", pa.string(), nullable=False),
    pa.field("openfda", pa.string(), nullable=True),
    pa.field("product_type", pa.string(), nullable=True),
    pa.field("event_id", pa.string(), nullable=True),
    pa.field("recalling_firm", pa.string(), nullable=True),
    pa.field("address_1", pa.string(), nullable=True),
    pa.field("address_2", pa.string(), nullable=True),
    pa.field("postal_code", pa.string(), nullable=True),
    pa.field("voluntary_mandated", pa.string(), nullable=True),
    pa.field("initial_firm_notification", pa.string(), nullable=True),
    pa.field("distribution_pattern", pa.string(), nullable=True),
    pa.field("recall_number", pa.string(), nullable=False),
    pa.field("product_description", pa.string(), nullable=True),
    pa.field("product_quantity", pa.string(), nullable=True),
    pa.field("reason_for_recall", pa.string(), nullable=True),
    pa.field("recall_initiation_date", pa.string(), nullable=True),
    pa.field("center_classification_date", pa.string(), nullable=True),
    pa.field("termination_date", pa.string(), nullable=True),
    pa.field("report_date", pa.string(), nullable=False),
    pa.field("code_info", pa.string(), nullable=True),
    pa.field("more_code_info", pa.string(), nullable=True),
    pa.field("meta_last_updated", pa.string(), nullable=True),
    pa.field("meta_results_skip", pa.int32(), nullable=True),
    pa.field("meta_results_limit", pa.int32(), nullable=True),
    pa.field("meta_results_total", pa.int32(), nullable=True),
    pa.field("_expected_total", pa.int32(), nullable=True),
    pa.field("_base_api", pa.string(), nullable=True),
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

SCHEMA_REGISTRY = {
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
        "id_column": ("EventNumber", "ProductGroupCode"),
        "description": "Global Dairy Trade event prices",
    },
    "market_indices": {
        "source_name": "market_indices",
        "table_name": "market_indices",
        "bronze": BRONZE_MARKET_INDICES_SCHEMA,
        "bronze_variants": (
            BRONZE_MARKET_INDICES_SCHEMA,
            BRONZE_MARKET_INDICES_WIDE_SCHEMA,
        ),
        "silver": MARKET_INDICES_SCHEMA,
        "partition": PARTITION_COLUMNS["market_indices"],
        "id_column": ("source", "index_name", "period_start"),
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
        "id_column": "recall_number",
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


def get_bronze_schema_variants(table_name: str) -> tuple[pa.Schema, ...]:
    """Return every accepted raw Bronze shape for a supported table."""
    table_name = normalize_table_name(table_name)
    if table_name not in SCHEMA_REGISTRY:
        raise ValueError(f"Unknown table: {table_name}")
    registry_entry = SCHEMA_REGISTRY[table_name]
    return tuple(registry_entry.get("bronze_variants", (registry_entry["bronze"],)))


def get_silver_schema(table_name: str) -> pa.Schema:
    """Get Silver schema for a table."""
    table_name = normalize_table_name(table_name)
    if table_name in UNSUPPORTED_SILVER_TABLES:
        raise ValueError(
            f"Unsupported Silver table '{table_name}': "
            f"{UNSUPPORTED_SILVER_TABLES[table_name]}"
        )
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
    if table_name in UNSUPPORTED_SILVER_TABLES:
        return table_name
    if table_name not in SCHEMA_REGISTRY:
        raise ValueError(f"Unknown table: {table_name}")
    return SCHEMA_REGISTRY[table_name]["source_name"]


def get_table_name(table_name: str) -> str:
    """Get Delta table name for a table."""
    table_name = normalize_table_name(table_name)
    if table_name not in SCHEMA_REGISTRY:
        raise ValueError(f"Unknown table: {table_name}")
    return SCHEMA_REGISTRY[table_name]["table_name"]


def get_id_column(table_name: str) -> str | tuple[str, ...]:
    """Get the primary ID column or composite business key for a table."""
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
