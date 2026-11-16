"""Freeze the core contracts used by downstream callers."""
import inspect
import os
import subprocess
import sys

import lakehouse_storage as storage

SIGNATURES = {'write_bronze_batch': '(df: polars.dataframe.frame.DataFrame, table_name: str, bronze_root: str | pathlib.Path | None = None, validate_schema: bool = True) -> str', 'append_to_delta': '(df: polars.dataframe.frame.DataFrame, table_uri: str, partition_by: list[str], table_name: str | None = None) -> None', 'ingest_bronze_file': '(bronze_path: str | pathlib.Path, silver_table_uri: str, partition_by: list[str], table_name: str | None = None, bronze_root: pathlib.Path | str | None = None, enforce_checksum: bool = True) -> dict', 'trace_batch': "(table_uri: str, batch_id: str, id_column: str = 'batch_id') -> dict", 'get_version_at_date': '(table_uri: str, target_date: str) -> int | None', 'compact_table': "(table_uri: 'str', target_file_size_mb: 'int' = 128) -> 'dict'", 'vacuum_table': "(table_uri: 'str', retention_hours: 'int' = 168, dry_run: 'bool' = True, enforce_retention_duration: 'bool' = False) -> 'dict'"}
SCHEMAS = {'sensor_telemetry': 'timestamp: timestamp[us, tz=UTC] not null\nline_id: string not null\nbatch_id: string\npreheat_temp: float\nuht_temp: float not null\nhomo_press_stage1: float\nhomo_press_stage2: float\nflow_rate: float\nconductivity: float\npower_kw: float\nis_anomaly_flag: bool', 'mes_lims': 'batch_id: string not null\nline_id: string not null\nproduction_start: timestamp[us, tz=UTC]\nproduction_end: timestamp[us, tz=UTC]\nsku_id: string not null\nfat_in: float\nprotein_in: float\nacidity_sh: float\nyield_recovery_pct: float\nmicrobiology_status: string\nbatch_defect_flag: int8 not null', 'market_prices': 'source: string not null\nproduct: string not null\ncontract: string\nobserved_at: timestamp[us, tz=UTC] not null\nprice: float\ncurrency: string not null\nunit: string\ningested_at: timestamp[us, tz=UTC] not null', 'market_indices': 'source: string not null\nindex_name: string not null\nperiod_start: date32[day] not null\nindex_value: float\nbase_period: string\ningested_at: timestamp[us, tz=UTC] not null', 'weather': 'farm_id: string not null\nobserved_at: timestamp[us, tz=UTC] not null\ntemperature_c: float\nrelative_humidity_pct: float\nprecipitation_mm: float\nthi_index: float\nis_heat_stress: bool\ningested_at: timestamp[us, tz=UTC] not null', 'food_recalls': 'source: string not null\nrecall_id: string not null\npublished_at: timestamp[us, tz=UTC] not null\nproduct: string\nreason: string\nstatus: string\nclassification: string not null\nsource_url: string\ningested_at: timestamp[us, tz=UTC] not null', 'bronze_metadata': 'source: string not null\nurl: string\nfinal_url: string\nfetched_at: timestamp[us, tz=UTC] not null\nsha256: string not null\nbytes: int64\ncontent_type: string\nfile: string not null'}


def test_existing_contracts():
    for name, signature in SIGNATURES.items():
        assert str(inspect.signature(getattr(storage, name))) == signature
    for name, schema in SCHEMAS.items():
        assert str(storage.get_silver_schema(name)) == schema
    assert storage.get_partition_columns("sensor_telemetry") == ["line_id", "ingestion_date"]
    assert all(hasattr(storage, name) for name in storage.__all__)


def test_core_import_without_optional_services():
    code = """
import sys
from importlib.abc import MetaPathFinder
class Block(MetaPathFinder):
    def find_spec(self, fullname, *args):
        if fullname.split('.')[0] in {'fastapi', 'uvicorn', 'apscheduler', 'matplotlib', 'data_governance'}:
            raise ImportError('optional import: ' + fullname)
sys.meta_path.insert(0, Block())
import lakehouse_storage
"""
    subprocess.run([sys.executable, "-c", code], env={**os.environ, "PYTHONPATH": "src"}, check=True)
