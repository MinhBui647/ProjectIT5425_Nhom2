"""Publication must expose only complete pairs and never replace raw data."""
import os
from pathlib import Path
import subprocess
import sys

import polars as pl
import pytest

from lakehouse_storage import bronze_writer as bronze
from test_storage_regressions import sensor


def test_same_clock_two_processes_keep_both_batches(tmp_path):
    code = r'''
import sys, time
from datetime import datetime, timezone
from pathlib import Path
import polars as pl
from lakehouse_storage import bronze_writer as b
class Clock(datetime):
    @classmethod
    def now(cls, tz=None):
        return cls(2026, 1, 15, 10, tzinfo=timezone.utc)
b.datetime = Clock
root, worker = Path(sys.argv[1]), sys.argv[2]
original = pl.DataFrame.write_parquet
def synchronized(self, path, *args, **kwargs):
    (root / ('ready-' + worker)).touch()
    deadline = time.monotonic() + 15
    while len(list(root.glob('ready-*'))) != 2:
        if time.monotonic() > deadline: raise RuntimeError('publication barrier timeout')
        time.sleep(0.01)
    return original(self, path, *args, **kwargs)
pl.DataFrame.write_parquet = synchronized
df = pl.DataFrame({'timestamp':['2026-01-15T10:00:00Z'], 'line_id':['LINE_UHT_1'],
    'uht_temp':[float(worker)], '_source_system':['TEST'], '_generated_at':['2026-01-15T10:00:00Z']})
print(b.write_bronze_batch(df, 'sensor_telemetry', root), flush=True)
'''
    processes = [subprocess.Popen(
        [sys.executable, '-c', code, str(tmp_path), str(i)],
        stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True,
        env={**os.environ, 'PYTHONPATH': 'src', 'POLARS_MAX_THREADS': '2'},
    ) for i in (1, 2)]
    for process in processes:
        stdout, stderr = process.communicate(timeout=25)
        assert process.returncode == 0, (stdout, stderr)
    files = bronze.list_bronze_files('sensor_telemetry', tmp_path)
    assert len(files) == 2
    assert all(bronze.verify_checksum(path) for path in files)
    assert sorted(pl.read_parquet(path)['uht_temp'][0] for path in files) == [1.0, 2.0]
    from data_governance.bronze_loader import load_bronze
    for path in files:
        _, identity = load_bronze(path, tmp_path)
        assert identity.source_timestamp.isoformat() == '2026-01-15T10:00:00+00:00'


@pytest.mark.parametrize('boundary', ['serialize', 'checksum', 'sidecar', 'parquet'])
def test_failures_do_not_expose_incomplete_parquet(tmp_path, monkeypatch, boundary):
    original_path = Path(bronze.write_bronze_batch(sensor(), 'sensor_telemetry', tmp_path))
    original_bytes = original_path.read_bytes()
    original_hash = original_path.with_suffix('.sha256').read_bytes()

    def fail(*args, **kwargs):
        raise OSError('injected publication failure')

    if boundary == 'serialize':
        original = pl.DataFrame.write_parquet
        def partial(self, path, *args, **kwargs):
            Path(path).write_bytes(b'partial parquet')
            fail()
        monkeypatch.setattr(pl.DataFrame, 'write_parquet', partial)
    elif boundary == 'checksum':
        monkeypatch.setattr(bronze, '_write_checksum', fail)
    else:
        link = os.link
        def failing_link(src, dst, *args, **kwargs):
            if Path(dst).suffix == ('.sha256' if boundary == 'sidecar' else '.parquet'):
                fail()
            return link(src, dst, *args, **kwargs)
        monkeypatch.setattr(os, 'link', failing_link)

    with pytest.raises(OSError, match='injected'):
        bronze.write_bronze_batch(sensor(), 'sensor_telemetry', tmp_path)
    assert bronze.list_bronze_files('sensor_telemetry', tmp_path) == [original_path]
    assert list(tmp_path.rglob('*.parquet')) == [original_path]
    assert original_path.read_bytes() == original_bytes
    assert original_path.with_suffix('.sha256').read_bytes() == original_hash


@pytest.mark.parametrize('boundary', ['checksum', 'parquet'])
def test_process_death_before_commit_exposes_no_parquet(tmp_path, boundary):
    code = r'''
import os, sys
from pathlib import Path
import polars as pl
from lakehouse_storage import bronze_writer as b
if sys.argv[2] == 'checksum':
    b._write_checksum = lambda path: os._exit(91)
else:
    link = os.link
    def crash(src, dst, *args, **kwargs):
        if Path(dst).suffix == '.parquet': os._exit(91)
        return link(src, dst, *args, **kwargs)
    os.link = crash
df = pl.DataFrame({'timestamp':['2026-01-15T10:00:00Z'], 'line_id':['LINE_UHT_1'],
    'uht_temp':[140.0], '_source_system':['TEST'], '_generated_at':['2026-01-15T10:00:00Z']})
b.write_bronze_batch(df, 'sensor_telemetry', sys.argv[1])
'''
    result = subprocess.run([sys.executable, '-c', code, str(tmp_path), boundary],
                            env={**os.environ, 'PYTHONPATH': 'src'}, timeout=20)
    assert result.returncode == 91
    assert list(tmp_path.rglob('*.parquet')) == []


def test_legacy_pair_is_readable(tmp_path):
    path = tmp_path / 'sensor_telemetry' / '2026-01-15' / 'sensor_telemetry_20260115_100000_000000.parquet'
    path.parent.mkdir(parents=True)
    sensor().write_parquet(path)
    bronze._write_checksum(path)
    assert bronze.verify_checksum(path)
    assert bronze.read_bronze_file(path, tmp_path).equals(sensor())
