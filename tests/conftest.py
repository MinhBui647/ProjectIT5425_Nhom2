"""Pytest configuration and fixtures for lakehouse storage tests.

Add src to sys.path so tests can use: from lakehouse_storage import ...
"""

import sys
from pathlib import Path

import pytest

# Add src to path for imports
SRC_PATH = Path(__file__).resolve().parents[1] / "src"
if str(SRC_PATH) not in sys.path:
    sys.path.insert(0, str(SRC_PATH))


# =============================================================================
# Storage Fixtures
# =============================================================================

@pytest.fixture
def storage_options():
    """MinIO storage options for Delta Lake operations."""
    return {
        "AWS_ENDPOINT_URL": "http://localhost:9000",
        "AWS_ACCESS_KEY_ID": "dairy",
        "AWS_SECRET_ACCESS_KEY": "hustit5425",
        "AWS_ALLOW_HTTP": "true",
        "AWS_S3_ALLOW_UNSAFE_RENAME": "true",
    }


# =============================================================================
# Path Fixtures
# =============================================================================

@pytest.fixture
def bronze_temp_path(tmp_path):
    """Temporary Bronze directory for tests."""
    bronze_path = tmp_path / "bronze_vault"
    bronze_path.mkdir(parents=True, exist_ok=True)
    return bronze_path


@pytest.fixture
def silver_temp_path(tmp_path):
    """Temporary Silver directory (local Delta) for tests."""
    silver_path = tmp_path / "silver_curated"
    silver_path.mkdir(parents=True, exist_ok=True)
    return silver_path


# =============================================================================
# Sample Data Fixtures
# =============================================================================

@pytest.fixture
def sample_sensor_batch():
    """Generate 100 rows of sensor data for testing."""
    from datetime import datetime, timedelta

    n = 100
    base_time = datetime(2026, 1, 15, 10, 0, 0)
    timestamps = [(base_time - timedelta(seconds=i)).isoformat() for i in range(n)]

    return {
        "timestamp": timestamps,
        "line_id": ["LINE_UHT_1"] * n,
        "batch_id": [None] * n,
        "preheat_temp": [75.0 + (i % 10) * 0.5 for i in range(n)],
        "uht_temp": [138.5] * n,
        "homo_press_stage1": [200.0] * n,
        "homo_press_stage2": [30.0] * n,
        "flow_rate": [5000.0] * n,
        "conductivity": [4.5] * n,
        "power_kw": [150.0] * n,
        "_source_system": ["TEST_SENSOR"] * n,
        "_generated_at": [datetime.now().isoformat()] * n,
    }


@pytest.fixture
def sample_mes_batch():
    """Generate 10 batches of MES data for testing."""
    from datetime import datetime, timedelta

    batches = []
    for i in range(10):
        start_time = datetime(2026, 1, 15, 6 + (i % 3) * 8, 0, 0)
        batches.append({
            "batch_id": f"TEST_BATCH_{i:04d}",
            "line_id": ["LINE_UHT_1", "LINE_UHT_2"][i % 2],
            "production_start": [start_time.isoformat()],
            "production_end": [(start_time + timedelta(hours=8)).isoformat()],
            "sku_id": ["UHT_PURE", "UHT_SWEETENED", "UHT_LOWFAT"][i % 3],
            "fat_in": [3.5 + (i % 5) * 0.1],
            "protein_in": [3.2 + (i % 4) * 0.1],
            "acidity_sh": [6.5 + (i % 3) * 0.1],
            "yield_recovery_pct": [92.5 + (i % 7) * 0.2],
            "microbiology_status": ["PASSED", "FAILED"][i % 4 == 0],
            "batch_defect_flag": [0 if i % 4 != 0 else 1],
            "_source_system": ["TEST_MES"],
            "_generated_at": ["2026-01-15T10:00:00"],
        })

    import polars as pl
    return pl.concat([pl.DataFrame(b) for b in batches])


@pytest.fixture
def sample_weather_batch():
    """Generate 100 rows of weather data for testing."""
    import numpy as np
    import polars as pl
    from datetime import datetime

    n = 100
    timestamps = [datetime(2026, 1, 15, 10, 0, 0).isoformat()] * n

    temps = np.random.normal(28.0, 5.0, n).tolist()
    humidity = np.random.normal(75.0, 10.0, n).tolist()

    return pl.DataFrame({
        "farm_id": ["MOC_CHAU"] * n,
        "observed_at": timestamps,
        "temperature_c": temps,
        "relative_humidity_pct": humidity,
        "precipitation_mm": np.abs(np.random.normal(0, 5, n)).tolist(),
        "ingested_at": [datetime.now().isoformat()] * n,
    })


@pytest.fixture
def sample_market_prices_batch():
    """Generate 50 rows of market prices for testing."""
    from datetime import datetime

    n = 50
    timestamps = [datetime(2026, 1, 15, 14, 0, 0).isoformat()] * n

    return {
        "source": ["GDT"] * n,
        "product": [["WMP", "SMP", "AMF"][i % 3] for i in range(n)],
        "contract": ["SPOT"] * n,
        "observed_at": timestamps,
        "price": [3500.0 + i * 10 for i in range(n)],
        "currency": ["USD"] * n,
        "unit": ["metric_ton"] * n,
        "ingested_at": [datetime.now().isoformat()] * n,
        "_source_system": ["TEST_MARKET"] * n,
        "_generated_at": [datetime.now().isoformat()] * n,
    }


@pytest.fixture
def sample_food_recalls_batch():
    """Generate 10 rows of food recall data for testing."""
    from datetime import datetime

    return {
        "source": ["FDA"] * 10,
        "recall_id": [f"TEST_RECALL_{i:04d}" for i in range(10)],
        "published_at": ["2026-01-15T10:00:00"] * 10,
        "product": ["Dairy Product"] * 10,
        "reason": ["Potential contamination"] * 10,
        "status": ["Ongoing"] * 10,
        "source_url": ["https://example.com"] * 10,
        "ingested_at": [datetime.now().isoformat()] * 10,
        "_source_system": ["TEST_FOOD"] * 10,
        "_generated_at": [datetime.now().isoformat()] * 10,
    }


# =============================================================================
# Polars DataFrame Fixtures
# =============================================================================

@pytest.fixture
def sensor_df(sample_sensor_batch):
    """Convert sample sensor batch to Polars DataFrame."""
    import polars as pl
    return pl.DataFrame(sample_sensor_batch)


@pytest.fixture
def mes_df(sample_mes_batch):
    """Get sample MES DataFrame."""
    return sample_mes_batch


# =============================================================================
# Schema Fixtures
# =============================================================================

@pytest.fixture
def bronze_sensor_schema():
    """Get Bronze sensor_telemetry schema."""
    from lakehouse_storage.schemas import BRONZE_SENSOR_TELEMETRY_SCHEMA
    return BRONZE_SENSOR_TELEMETRY_SCHEMA


@pytest.fixture
def silver_sensor_schema():
    """Get Silver sensor_telemetry schema."""
    from lakehouse_storage.schemas import SENSOR_TELEMETRY_SCHEMA
    return SENSOR_TELEMETRY_SCHEMA


# =============================================================================
# Helper Functions
# =============================================================================

from lakehouse_storage.utils import sha256_file  # noqa: E402


# Expose helper for use in tests
@pytest.fixture
def hash_file():
    """Fixture that provides SHA-256 hashing function."""
    return sha256_file
