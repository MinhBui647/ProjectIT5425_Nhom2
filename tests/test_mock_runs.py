"""Persisted mock payloads must survive process restarts unchanged."""
import hashlib
import json
import os
import subprocess
import sys
from datetime import datetime, timedelta

import pytest

from lakehouse_storage.mock_server.models import RunRequest
from lakehouse_storage.mock_server.store import RunStore, canonical_json


def test_persisted_run_and_sensor_mes_link(tmp_path):
    root = tmp_path / 'storage_demo' / 'mock'
    store = RunStore(root)
    run = store.create(RunRequest(samples_per_batch=12, batches=2, seed=42))
    envelopes = [json.loads(store.batch(run['run_id'], i)) for i in range(3)]
    mes = envelopes[0]['data'][0]
    sensor = envelopes[1]
    assert sensor['table_name'] == 'sensor_telemetry'
    assert hashlib.sha256(canonical_json(sensor['data'])).hexdigest() == sensor['payload_sha256']
    for env in envelopes[1:]:
        for row in env['data']:
            assert row['batch_id'] == mes['batch_id']
            assert row['line_id'] == mes['line_id']
            assert datetime.fromisoformat(mes['production_start']) <= datetime.fromisoformat(row['timestamp']) < datetime.fromisoformat(mes['production_end'])
    times = [datetime.fromisoformat(r['timestamp']) for e in envelopes[1:] for r in e['data']]
    assert all(b - a == timedelta(seconds=1) for a, b in zip(times, times[1:]))
    code = "from lakehouse_storage.mock_server.store import RunStore; from pathlib import Path; import sys; sys.stdout.buffer.write(RunStore(Path(sys.argv[1])).batch(sys.argv[2],1))"
    result = subprocess.run([sys.executable, '-c', code, str(root), run['run_id']], capture_output=True, check=True, env={**os.environ, 'PYTHONPATH': 'src'})
    assert result.stdout == store.batch(run['run_id'], 1)


@pytest.mark.parametrize('kwargs', [{'samples_per_batch': 0}, {'samples_per_batch': 50001}, {'batches': 201}, {'batches': 200, 'samples_per_batch': 50000}, {'delay_seconds': 6}, {'scenario': 'unknown'}])
def test_run_limits(kwargs):
    with pytest.raises(ValueError): RunRequest(**kwargs)


def test_missing_run_or_sequence(tmp_path):
    store = RunStore(tmp_path)
    with pytest.raises(ValueError): store.run('../escape')
    with pytest.raises(FileNotFoundError): store.run('a' * 32)
    run = store.create(RunRequest(samples_per_batch=2, batches=1))
    with pytest.raises(FileNotFoundError): store.batch(run['run_id'], 2)


def test_batch_size_does_not_change_logical_sensor_samples(tmp_path):
    store = RunStore(tmp_path)
    runs = [store.create(RunRequest(samples_per_batch=size, batches=12//size)) for size in (3,6)]
    datasets = []
    for run in runs:
        rows = [row for seq in range(1, run['batch_count']) for row in json.loads(store.batch(run['run_id'],seq))['data']]
        datasets.append([{k:v for k,v in row.items() if k != 'batch_id'} for row in rows])
    assert datasets[0] == datasets[1]
