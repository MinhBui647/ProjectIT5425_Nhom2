"""LW02 acceptance: run inside the pinned Airflow image, never importorskip."""

import importlib.metadata
import inspect
import json
import os
from pathlib import Path
import subprocess

import pytest


pytestmark = pytest.mark.skipif(
    os.environ.get("LAKEHOUSE_AIRFLOW_TEST") != "1",
    reason="run with LAKEHOUSE_AIRFLOW_TEST=1 inside the pinned Airflow image",
)


def _processing_smoke(root):
    import json
    from pathlib import Path

    import polars as pl
    from deltalake import DeltaTable
    from data_governance.run_governance import process_bronze_file
    from lakehouse_storage.bronze_writer import write_bronze_batch
    from lakehouse_storage.config import get_storage_options
    from lakehouse_storage.governance_adapter import ingest_governed_bronze_file

    root = Path(root)
    root.mkdir(parents=True, exist_ok=True)
    artifact = root / "artifact.json"
    artifact.write_text(json.dumps({"window": "2026-01-15T00:00:00Z"}))
    assert json.loads(artifact.read_text())["window"] == "2026-01-15T00:00:00Z"
    assert callable(process_bronze_file)
    fixture = next(Path("/opt/project/tests/fixtures/bronze/sensor_telemetry").glob("*.parquet"))
    bronze = write_bronze_batch(pl.read_parquet(fixture), "sensor_telemetry", root / "bronze")
    result = ingest_governed_bronze_file(
        bronze, "sensor_telemetry", bronze_root=root / "bronze",
        silver_root=str(root / "silver"), reports_root=root / "reports",
    )
    assert result["status"] == "PASS", result
    table = DeltaTable(str(root / "silver" / "sensor_telemetry"), storage_options=get_storage_options())
    rows = pl.from_arrow(table.to_pyarrow_table()).height
    assert rows == result["rows_written"] > 0
    return {"rows": rows, "version": table.version(), "artifact": str(artifact)}


def _call_processing(root):
    script = "import json, sys\n" + inspect.getsource(_processing_smoke)
    script += "\nprint('LW02_RESULT=' + json.dumps(_processing_smoke(sys.argv[1])))\n"
    process = subprocess.run(
        [os.environ.get("PROCESSING_PYTHON", "/opt/processing/bin/python"), "-c", script, root],
        capture_output=True, text=True, check=True, timeout=120,
    )
    line = next(line for line in process.stdout.splitlines() if line.startswith("LW02_RESULT="))
    return json.loads(line.removeprefix("LW02_RESULT="))


def test_airflow_and_standard_provider_are_pinned():
    assert importlib.metadata.version("apache-airflow") == "3.1.7"
    assert importlib.metadata.version("apache-airflow-providers-standard") == "1.11.0"
    assert Path(os.environ.get("PROCESSING_PYTHON", "/opt/processing/bin/python")).is_file()


def test_task_calls_processing_and_reads_governed_demo_delta(tmp_path):
    from airflow.providers.standard.operators.python import PythonOperator

    root = tmp_path / "storage_demo" / "lw02"
    operator = PythonOperator(
        task_id="environment_smoke", python_callable=_call_processing,
        op_args=[str(root)], do_xcom_push=False,
    )
    result = operator.execute(context={})
    assert result["rows"] > 0
    assert result["version"] == 0
    assert Path(result["artifact"]).is_file()


def test_processing_core_import_has_no_airflow_dependency():
    code = """
import sys
from importlib.abc import MetaPathFinder
class Block(MetaPathFinder):
    def find_spec(self, fullname, *args):
        if fullname.split('.')[0] == 'airflow':
            raise ImportError('Airflow is not a core dependency')
sys.meta_path.insert(0, Block())
import lakehouse_storage
import data_governance.run_governance
"""
    subprocess.run(
        [os.environ.get("PROCESSING_PYTHON", "/opt/processing/bin/python"), "-c", code],
        check=True, timeout=60,
    )


def test_postgres_async_driver_available_for_api_server():
    import asyncpg
    assert asyncpg.__version__
