"""Stable defaults for the Bronze-to-Silver governance pipeline."""

from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[2]

DEFAULT_ERROR_THRESHOLD = 0.005
CONTRACT_VERSION = "v1"

HAMPEL_WINDOW_RADIUS = 5
HAMPEL_THRESHOLD = 3.0
ROLLING_Z_WINDOW = 30
ROLLING_Z_THRESHOLD = 3.0

REPORTS_ROOT = PROJECT_ROOT / "reports"
DATA_DOCS_PATH = REPORTS_ROOT / "data_docs"
QUALITY_REPORTS_PATH = REPORTS_ROOT / "data_quality"
FIGURES_PATH = REPORTS_ROOT / "figures"
QUARANTINE_PATH = PROJECT_ROOT / "data" / "quality_quarantine"
LINEAGE_SPOOL_PATH = REPORTS_ROOT / "lineage_spool"

