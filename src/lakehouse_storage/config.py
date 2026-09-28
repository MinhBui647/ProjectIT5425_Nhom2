"""Configuration for Smart Dairy Factory Lakehouse.

All config comes from environment variables. Never hardcode secrets.
"""

import os

# =============================================================================
# Timezone Configuration
# =============================================================================

TIMEZONE = os.getenv("FACTORY_TIMEZONE", "UTC")

# =============================================================================
# MinIO Configuration (matches docker-compose.minio.yml)
# =============================================================================

MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "localhost:9000")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY", "dairy")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY", "hustit5425")
MINIO_SECURE = os.getenv("MINIO_SECURE", "false").lower() == "true"
MINIO_BUCKET = os.getenv("MINIO_BUCKET", "smart-dairy-lakehouse")

# =============================================================================
# Storage Options (for Delta Lake S3 operations)
# =============================================================================

STORAGE_OPTIONS = {
    "AWS_ENDPOINT_URL": f"{'https' if MINIO_SECURE else 'http'}://{MINIO_ENDPOINT}",
    "AWS_ACCESS_KEY_ID": MINIO_ACCESS_KEY,
    "AWS_SECRET_ACCESS_KEY": MINIO_SECRET_KEY,
    "AWS_ALLOW_HTTP": "true" if not MINIO_SECURE else "false",
    "AWS_S3_ALLOW_UNSAFE_RENAME": "true",
}


def get_storage_options(
    endpoint: str | None = None,
    access_key: str | None = None,
    secret_key: str | None = None,
) -> dict:
    """Get storage options for Delta Lake S3 operations.

    Args:
        endpoint: S3 endpoint URL (default: from config)
        access_key: AWS access key (default: from config)
        secret_key: AWS secret key (default: from config)

    Returns:
        Dict suitable for deltalake storage_options parameter
    """
    return {
        "AWS_ENDPOINT_URL": endpoint or f"{'https' if MINIO_SECURE else 'http'}://{MINIO_ENDPOINT}",
        "AWS_ACCESS_KEY_ID": access_key or MINIO_ACCESS_KEY,
        "AWS_SECRET_ACCESS_KEY": secret_key or MINIO_SECRET_KEY,
        "AWS_ALLOW_HTTP": "true" if not MINIO_SECURE else "false",
        "AWS_S3_ALLOW_UNSAFE_RENAME": "true",
    }

# =============================================================================
# Path Configuration
# =============================================================================

# Local Bronze vault (immutable raw landing)
BRONZE_LOCAL_PATH = os.getenv("BRONZE_LOCAL_PATH", "data/01_bronze_vault")

# Local Silver snapshot (for export only, not primary storage)
SILVER_LOCAL_SNAPSHOT_PATH = os.getenv(
    "SILVER_LOCAL_SNAPSHOT_PATH", "data/02_silver_curated"
)

# Local Gold mart (analytics snapshots)
GOLD_LOCAL_PATH = os.getenv("GOLD_LOCAL_PATH", "data/03_gold_marts")

# =============================================================================
# Delta Lake Table URIs (on MinIO)
# =============================================================================

SILVER_TABLE_URIS = {
    "sensor_telemetry": f"s3://{MINIO_BUCKET}/02_silver_curated/sensor_telemetry",
    "mes_lims": f"s3://{MINIO_BUCKET}/02_silver_curated/mes_lims",
    "market_prices": f"s3://{MINIO_BUCKET}/02_silver_curated/market_prices",
    "market_indices": f"s3://{MINIO_BUCKET}/02_silver_curated/market_indices",
    "weather": f"s3://{MINIO_BUCKET}/02_silver_curated/weather",
    "food_recalls": f"s3://{MINIO_BUCKET}/02_silver_curated/food_recalls",
    "bronze_metadata": f"s3://{MINIO_BUCKET}/02_silver_curated/bronze_metadata",
    "S1": f"s3://{MINIO_BUCKET}/02_silver_curated/sensor_telemetry",
    "S2": f"s3://{MINIO_BUCKET}/02_silver_curated/mes_lims",
    "S3": f"s3://{MINIO_BUCKET}/02_silver_curated/market_prices",
    "S4": f"s3://{MINIO_BUCKET}/02_silver_curated/weather",
    "S5": f"s3://{MINIO_BUCKET}/02_silver_curated/food_recalls",
    "fact_sensor_telemetry": f"s3://{MINIO_BUCKET}/02_silver_curated/sensor_telemetry",
    "fact_batch_production": f"s3://{MINIO_BUCKET}/02_silver_curated/mes_lims",
    "fact_market_data": f"s3://{MINIO_BUCKET}/02_silver_curated/market_prices",
    "fact_market_indices": f"s3://{MINIO_BUCKET}/02_silver_curated/market_indices",
    "fact_farm_weather": f"s3://{MINIO_BUCKET}/02_silver_curated/weather",
    "fact_food_safety": f"s3://{MINIO_BUCKET}/02_silver_curated/food_recalls",
}

SILVER_TABLE_URI = SILVER_TABLE_URIS["sensor_telemetry"]

# Throwaway table for testing/traceability
TRACEABILITY_TABLE_URI = f"s3://{MINIO_BUCKET}/02_silver_curated/_traceability"

# =============================================================================
# Maintenance Configuration
# =============================================================================

# Vacuum retention hours
VACUUM_RETENTION_HOURS_DEV = 0  # Only for testing
VACUUM_RETENTION_HOURS_SAFE = 168  # 7 days - safe default for production

# File size thresholds for compaction
COMPACTION_MIN_FILE_SIZE_MB = 128  # Files smaller than this will be compacted

# =============================================================================
# Data Generation Defaults
# =============================================================================

DEFAULT_LINE_IDS = ["LINE_UHT_1", "LINE_UHT_2"]
DEFAULT_SKU_CODES = ["UHT_PURE", "UHT_SWEETENED", "UHT_LOWFAT"]


def get_table_uri(table_name: str) -> str:
    """Get S3 URI for a table by name."""
    if table_name in SILVER_TABLE_URIS:
        return SILVER_TABLE_URIS[table_name]
    # If not found, construct from pattern
    return f"s3://{MINIO_BUCKET}/02_silver_curated/{table_name}"
