"""The public governed path uses P3 results without bypassing its gate."""
import polars as pl
import pytest

from lakehouse_storage.bronze_writer import write_bronze_batch
from lakehouse_storage.mock_sensor_stream import generate_sensor_batch


def process(path, tmp_path):
    from lakehouse_storage import ingest_governed_bronze_file
    return ingest_governed_bronze_file(
        path, 'sensor_telemetry', bronze_root=tmp_path / 'bronze',
        silver_root=str(tmp_path / 'silver'), reports_root=tmp_path / 'reports',
    )


def test_governed_accept_replay_and_gold(tmp_path):
    from olap_engineering.build_marts import scan_silver
    raw = generate_sensor_batch(20, seed=42, batch_id='BATCH_TEST')
    path = write_bronze_batch(raw, 'sensor_telemetry', tmp_path / 'bronze')
    result = process(path, tmp_path)
    assert result['status'] in {'PASS', 'PASS_WITH_REJECTIONS'}, result
    silver = scan_silver('sensor_telemetry', str(tmp_path / 'silver')).collect()
    assert silver.height == result['rows_written'] > 0
    for col in ('_contract_version', '_quality_status', '_quality_run_id'):
        assert silver[col].null_count() == 0
    assert process(path, tmp_path)['status'] == 'SKIPPED_DUPLICATE'
    assert scan_silver('sensor_telemetry', str(tmp_path / 'silver')).collect().height == silver.height


def test_governed_quality_failure_is_not_committed(tmp_path):
    raw = generate_sensor_batch(10, seed=42).with_columns(pl.lit(20.0).alias('uht_temp'))
    path = write_bronze_batch(raw, 'sensor_telemetry', tmp_path / 'bronze')
    result = process(path, tmp_path)
    assert result['status'] == 'FAIL'
    assert result['rows_written'] == 0
    assert not (tmp_path / 'silver' / 'sensor_telemetry').exists()


@pytest.mark.parametrize('missing', ['_contract_version', '_quality_status', '_quality_run_id'])
def test_governed_reports_legacy_schema_without_migrating(tmp_path, missing):
    from deltalake import write_deltalake, DeltaTable
    from lakehouse_storage.config import get_storage_options
    cols = {'_contract_version': ['v1'], '_quality_status': ['PASS'], '_quality_run_id': ['old']}
    del cols[missing]
    uri = str(tmp_path / 'silver' / 'sensor_telemetry')
    write_deltalake(uri, pl.DataFrame(cols).to_arrow(), storage_options=get_storage_options())
    path = write_bronze_batch(generate_sensor_batch(5, seed=42), 'sensor_telemetry', tmp_path / 'bronze')
    result = process(path, tmp_path)
    assert result['status'] == 'FAIL' and result['error_category'] == 'infrastructure'
    assert missing in result['error'] and 'legacy' in result['error'].lower()
    assert DeltaTable(uri, storage_options=get_storage_options()).version() == 0


def test_governed_infrastructure_failure_has_status(tmp_path, monkeypatch):
    import data_governance.run_governance as p3
    def fail(*args, **kwargs):
        raise OSError('offline storage')
    monkeypatch.setattr(p3, 'process_bronze_file', fail)
    path = write_bronze_batch(generate_sensor_batch(5, seed=42), 'sensor_telemetry', tmp_path / 'bronze')
    result = process(path, tmp_path)
    assert result['status'] == 'FAIL' and result['error_category'] == 'infrastructure'
    assert result['rows_written'] == 0 and 'offline storage' in result['error']


@pytest.mark.parametrize('module', ['mock_sensor_stream', 'generate_mes_data'])
def test_generator_cli_writes_governed_gold_input(tmp_path, module):
    import os
    import subprocess
    import sys
    from olap_engineering.build_marts import scan_silver
    args = ['--batches', '1', '--samples', '20', '--seed', '42'] if module == 'mock_sensor_stream' else [
        '--days', '1', '--date', '2026-01-15', '--seed', '5425', '--defect-rate', '0', '--write-delta']
    result = subprocess.run([
        sys.executable, '-B', '-m', 'lakehouse_storage.' + module, *args,
        '--bronze-root', str(tmp_path / 'bronze'), '--silver-root', str(tmp_path / 'silver'),
        '--reports-root', str(tmp_path / 'reports'),
    ], capture_output=True, text=True, timeout=90, env={**os.environ, 'PYTHONPATH': 'src'})
    assert result.returncode == 0, (result.stdout, result.stderr)
    tables = ['sensor_telemetry'] if module == 'mock_sensor_stream' else ['sensor_telemetry', 'mes_lims']
    for table in tables:
        frame = scan_silver(table, str(tmp_path / 'silver')).collect()
        assert frame.height > 0
        assert frame['_quality_run_id'].null_count() == 0


@pytest.mark.parametrize('module', ['mock_sensor_stream', 'generate_mes_data'])
def test_generator_cli_returns_failure_for_rejected_batch(tmp_path, monkeypatch, module):
    import importlib
    import sys
    cli = importlib.import_module('lakehouse_storage.' + module)
    raw = generate_sensor_batch(10, seed=42).with_columns(pl.lit(20.0).alias('uht_temp'))
    if module == 'mock_sensor_stream':
        monkeypatch.setattr(cli, 'generate_sensor_batch', lambda **kwargs: raw)
        args = ['--batches', '1']
    else:
        monkeypatch.setattr(cli, 'generate_daily_production', lambda **kwargs: (raw, pl.DataFrame(), {}))
        args = ['--days', '1', '--write-delta']
    monkeypatch.setattr(sys, 'argv', [module, *args, '--bronze-root', str(tmp_path / 'bronze'),
                        '--silver-root', str(tmp_path / 'silver'), '--reports-root', str(tmp_path / 'reports')])
    assert cli.main() == 1
    assert not (tmp_path / 'silver' / 'sensor_telemetry').exists()
